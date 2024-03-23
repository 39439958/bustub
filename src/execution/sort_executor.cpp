#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "binder/bound_order_by.h"
#include "execution/executors/limit_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), tuples_index_(0) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(std::move(tuple));
  }

  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    auto order_bys = plan_->GetOrderBy();
    const auto &schema = child_executor_->GetOutputSchema();
    for (auto &[order_type, expr] : order_bys) {
      Value va = expr->Evaluate(&a, schema);
      Value vb = expr->Evaluate(&b, schema);
      if (order_type == OrderByType::DESC) {
        if (va.CompareEquals(vb) == CmpBool::CmpTrue) {
          continue;
        }
        return va.CompareGreaterThan(vb);
      }
      if (va.CompareEquals(vb) == CmpBool::CmpTrue) {
        continue;
      }
      return va.CompareLessThan(vb);
    }
    return CmpBool::CmpTrue;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_index_ == tuples_.size()) {
    return false;
  }
  *tuple = tuples_[tuples_index_];
  ++tuples_index_;
  return true;
}

}  // namespace bustub
