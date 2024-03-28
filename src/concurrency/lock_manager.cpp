//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <memory>
#include <mutex>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
  // 1.check transaction state
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // check transaction isolation level
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // 2.get lock request queue
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    // if queue is not found, create quest and queue insert in table lock map
    std::shared_ptr<LockRequest> lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    lock_request->granted_ = true;
    std::shared_ptr<LockRequestQueue> lock_request_queue = std::make_shared<LockRequestQueue>();
    lock_request_queue->request_queue_.emplace_back(lock_request);
    table_lock_map_.emplace(oid, lock_request_queue);
    InsertOrDeleteTableLockSet(txn, lock_request, true);
    table_lock_map_latch_.unlock();
    return true;
  }
  auto lock_request_queue = table_lock_map_.at(oid);
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // 3.upgrading lock request
  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      // upgrading lock fail
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if ((lock_request->lock_mode_ == LockMode::SHARED && 
            (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE)) ||
          (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) ||
          (lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE)) ||
          (lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // upgrading lock success
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      lock_request_queue->request_queue_.remove(lock_request);
      InsertOrDeleteTableLockSet(txn, lock_request, false);  
      std::shared_ptr<LockRequest> upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      auto iter = lock_request_queue->request_queue_.begin();
      while (iter != lock_request_queue->request_queue_.end()) {
        if (!(*iter)->granted_) {
          break;
        }
        iter++;
      }
      lock_request_queue->request_queue_.insert(iter, upgrade_lock_request);

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      lock_request_queue->cv_.wait(lock, [&]() { return GrantLock(upgrade_lock_request, lock_request_queue); });

      upgrade_lock_request->granted_ = true;
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      InsertOrDeleteTableLockSet(txn, upgrade_lock_request, true);
      return true;
    }
  }

  // 4.common lock request
  std::shared_ptr<LockRequest> lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  lock_request_queue->cv_.wait(lock, [&]() { return GrantLock(lock_request, lock_request_queue); });

  lock_request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, lock_request, true);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  // 1.get lock request queue
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  
  // 2.check row lock 
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto lock_request_queue = table_lock_map_.at(oid);
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // 3.unlock
  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
        if (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED || txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      InsertOrDeleteTableLockSet(txn, lock_request, false);
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->latch_.unlock();
      lock_request_queue->cv_.notify_all();
      return true;
    }
  }

  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1.check lock type
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  // check transaction state
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // check transaction isolation level
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw AbortReason::LOCK_ON_SHRINKING;
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // check table lock
  if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedLocked(oid) &&
        !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  // 2.get lock request queue
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    // if queue is not found, create quest and queue insert in table lock map
    std::shared_ptr<LockRequest> lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    lock_request->granted_ = true;
    std::shared_ptr<LockRequestQueue> lock_request_queue = std::make_shared<LockRequestQueue>();
    lock_request_queue->request_queue_.emplace_back(lock_request);
    row_lock_map_.emplace(rid, lock_request_queue);
    InsertOrDeleteRowLockSet(txn, lock_request, true);
    row_lock_map_latch_.unlock();
    return true;
  }
  auto lock_request_queue = row_lock_map_.at(rid);
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // 3.upgrading lock request
  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      // upgrading lock fail
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw AbortReason::UPGRADE_CONFLICT;
      }
      if (lock_request->lock_mode_ == LockMode::EXCLUSIVE && lock_mode == LockMode::SHARED) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw AbortReason::INCOMPATIBLE_UPGRADE;
      }
      // upgrading lock success
      lock_request_queue->upgrading_ = txn->GetTransactionId(); 
      lock_request_queue->request_queue_.remove(lock_request);
      InsertOrDeleteRowLockSet(txn, lock_request, false); 
      std::shared_ptr<LockRequest> upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      auto iter = lock_request_queue->request_queue_.begin();
      while (iter != lock_request_queue->request_queue_.end()) {
        if (!(*iter)->granted_) {
          break;
        }
        iter++;
      }
      lock_request_queue->request_queue_.insert(iter, upgrade_lock_request);

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      lock_request_queue->cv_.wait(lock, [&]() { return GrantLock(upgrade_lock_request, lock_request_queue); });

      upgrade_lock_request->granted_ = true;
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      InsertOrDeleteRowLockSet(txn, upgrade_lock_request, true);
      return true;
    }
  }

  // 4.common lock request
  std::shared_ptr<LockRequest> lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  lock_request_queue->cv_.wait(lock, [&]() { return GrantLock(lock_request, lock_request_queue); });

  lock_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, lock_request, true);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  // 1.get lock request queue
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_.at(rid);
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // 3.unlock
  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
        if (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED || txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      InsertOrDeleteRowLockSet(txn, lock_request, false);
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->latch_.unlock();
      lock_request_queue->cv_.notify_all();
      return true;
    }
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request, const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (auto &lock_request_tmp : lock_request_queue->request_queue_) {
    if (lock_request_tmp.get() == lock_request.get()) {
      return true;
    }
    if ((lock_request_tmp->lock_mode_ == LockMode::INTENTION_SHARED && 
      (lock_request->lock_mode_ == LockMode::EXCLUSIVE)) || 
      (lock_request_tmp->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
      (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
      (lock_request_tmp->lock_mode_ == LockMode::SHARED &&
      (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
      (lock_request_tmp->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE &&
      (lock_request->lock_mode_ != LockMode::INTENTION_SHARED)) ||
      (lock_request_tmp->lock_mode_ == LockMode::EXCLUSIVE)){
    return false;
    }
  }
  return false;
}

auto LockManager::InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request, bool insert) -> void {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED :
      if (insert) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED :
      if (insert) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE :
      if (insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE :
      if (insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE :
      if (insert) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

auto LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request, bool insert) -> void {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED :
      if (insert) {
        auto s_row_set_it = s_row_lock_set->find(lock_request->oid_);
        if (s_row_set_it == s_row_lock_set->end()) {
          s_row_lock_set->emplace(lock_request->oid_, std::unordered_set<RID>{lock_request->rid_});
          return;
        }
        s_row_set_it->second.emplace(lock_request->rid_);
      } else {
        auto s_row_set_it = s_row_lock_set->find(lock_request->oid_);
        if (s_row_set_it == s_row_lock_set->end()) {
          return;
        }
        s_row_set_it->second.erase(lock_request->rid_);
      }
      break;
    case LockMode::EXCLUSIVE :
      if (insert) {
        auto x_row_set_it = x_row_lock_set->find(lock_request->oid_);
        if (x_row_set_it == x_row_lock_set->end()) {
          x_row_lock_set->emplace(lock_request->oid_, std::unordered_set<RID>{lock_request->rid_});
          return;
        }
        x_row_set_it->second.emplace(lock_request->rid_);
      } else {
        auto x_row_set_it = x_row_lock_set->find(lock_request->oid_);
        if (x_row_set_it == x_row_lock_set->end()) {
          return;
        }
        x_row_set_it->second.erase(lock_request->rid_);
      }
      break;
    default:
      break;
  }
}

}  // namespace bustub
