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
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (delete_flag_) {
    return false;
  }
  delete_flag_ = true;

  auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  int delete_count = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto *table_heap = table_info->table_.get();
    table_heap->MarkDelete(*rid, exec_ctx_->GetTransaction());
    for (const auto &index : index_info) {
      Tuple key_tuple =
          tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    ++delete_count;
  }
  std::vector<Value> values{};
  values.push_back({Value{INTEGER, delete_count}});
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
