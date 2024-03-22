//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      tuples_index_(0) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
    
  }
}

void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();

  std::vector<Tuple> left_tuples;
  std::vector<Tuple> right_tuples;
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples.emplace_back(std::move(tuple));
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples.emplace_back(std::move(tuple));
  }

  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  for (auto &left_tuple : left_tuples) {
    bool join_success = false;
    for (auto &right_tuple : right_tuples) {
      Value join_result = plan_->predicate_->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      if (!join_result.IsNull() && join_result.GetAs<bool>()) {
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
        join_success = true;
      }
    }
    if (!join_success && plan_->GetJoinType() == JoinType::LEFT) {
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
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_index_ == tuples_.size()) {
    return false;
  }
  *tuple = tuples_[tuples_index_];
  ++tuples_index_;
  return true;
}

}  // namespace bustub
