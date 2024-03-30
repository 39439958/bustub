//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_heap_(nullptr), table_iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      auto lock_success = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
      if (!lock_success) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed");
    }
  }
  auto info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = info->table_.get();
  table_iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == table_heap_->End()) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      try {
        auto s_row_lock_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(plan_->GetTableOid());
        for (auto row : s_row_lock_set) {
          auto lock_success =
              exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), row);
          if (!lock_success) {
            throw ExecutionException("SeqScan Executor Release Row Lock Failed");
          }
        }
        auto lock_success = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->GetTableOid());
        if (!lock_success) {
          throw ExecutionException("SeqScan Executor Release Table Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("SeqScan Executor Release Lock Failed");
      }
    }
    return false;
  }
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      auto lock_success = exec_ctx_->GetLockManager()->LockRow(
          exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), table_iter_->GetRid());
      if (!lock_success) {
        throw ExecutionException("SeqScan Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }
  *rid = table_iter_->GetRid();
  *tuple = *table_iter_++;
  return true;
}

}  // namespace bustub
