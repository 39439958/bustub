//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "storage/page/table_page.h"
#include "type/type.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), insert_flag_(false) {}

void InsertExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  try {
    auto lock_success = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!lock_success) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (insert_flag_) {
    return false;
  }
  insert_flag_ = true;

  int insert_count = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto *table_heap = table_info_->table_.get();
    try {
      auto lock_success = exec_ctx_->GetLockManager()->LockRow(
          exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), *rid);
      if (!lock_success) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("Insert Executor Get Row Lock Failed");
    }
    table_heap->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    for (const auto &index : index_info_) {
      Tuple key_tuple =
          tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
          {*rid, plan_->TableOid(), WType::INSERT, key_tuple, index->index_oid_, exec_ctx_->GetCatalog()});
    }
    ++insert_count;
  }
  std::vector<Value> values{};
  values.push_back({Value{INTEGER, insert_count}});
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
