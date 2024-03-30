//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "storage/index/index.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), delete_flag_(false) {}

void DeleteExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  try {
    auto lock_success = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!lock_success) {
      throw ExecutionException("Delete Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Delete Executor Get Table Lock Failed");
  }
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (delete_flag_) {
    return false;
  }
  delete_flag_ = true;

  int delete_count = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto *table_heap = table_info_->table_.get();
    try {
      auto lock_success = exec_ctx_->GetLockManager()->LockRow(
          exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), *rid);
      if (!lock_success) {
        throw ExecutionException("Delete Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("Delete Executor Get Row Lock Failed");
    }
    table_heap->MarkDelete(*rid, exec_ctx_->GetTransaction());
    for (const auto &index : index_info_) {
      Tuple key_tuple =
          tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
          {*rid, plan_->TableOid(), WType::DELETE, key_tuple, index->index_oid_, exec_ctx_->GetCatalog()});
    }
    ++delete_count;
  }
  std::vector<Value> values{};
  values.push_back({Value{INTEGER, delete_count}});
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
