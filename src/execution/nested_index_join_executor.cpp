//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tuples_index_(0) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { 
  child_executor_->Init();

  Tuple left_tuple;
  RID left_rid;
  auto left_schema = child_executor_->GetOutputSchema();
  auto right_schema = plan_->InnerTableSchema();
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto table_heap = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->table_.get();
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    Value left_key_value = plan_->KeyPredicate()->Evaluate(&left_tuple, left_schema);
    Tuple left_key_tuple = Tuple(std::vector<Value>{left_key_value}, &index_info->key_schema_);
    std::vector<RID> result;
    Tuple right_tuple;
    index_info->index_->ScanKey(left_key_tuple, &result, exec_ctx_->GetTransaction());
    if (result.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      auto left_count = left_schema.GetColumnCount();
      auto right_count = right_schema.GetColumnCount();
      for (uint32_t i = 0; i < left_count; i++) {
        values.push_back(left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_count; i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      tuples_.emplace_back(values, &plan_->OutputSchema());
    }
    if (!result.empty()) {
      // because index key is unique
      table_heap->GetTuple(result[0], &right_tuple, exec_ctx_->GetTransaction());
      std::vector<Value> values;
      auto left_count = left_schema.GetColumnCount();
      auto right_count = right_schema.GetColumnCount();
      for (uint32_t i = 0; i < left_count; i++) {
        values.push_back(left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_count; i++) {
        values.push_back(right_tuple.GetValue(&right_schema, i));
      }
      tuples_.emplace_back(values, &plan_->OutputSchema());
    }
  }

}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (tuples_index_ == tuples_.size()) {
    return false;
  }
  *tuple = tuples_[tuples_index_];
  ++tuples_index_;
  return true;
}

}  // namespace bustub
