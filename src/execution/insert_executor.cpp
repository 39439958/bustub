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
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (insert_flag_) {
    return false;
  }
  insert_flag_ = true;

  auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  int insert_count = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto *table_heap = table_info->table_.get();
    table_heap->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    for (const auto &index : index_info) {
      Tuple key_tuple =
          tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    ++insert_count;
  }
  std::vector<Value> values{};
  values.push_back({Value{INTEGER, insert_count}});
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
