#include "execution/executors/topn_executor.h"
#include <algorithm>
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), tuples_index_(0) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp = [this](const Tuple &a, const Tuple &b) {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    pq.push(tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    tuples_.push_back(pq.top());
    pq.pop();
  }
  std::reverse(tuples_.begin(), tuples_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_index_ == tuples_.size()) {
    return false;
  }
  *tuple = tuples_[tuples_index_++];
  return true;
}

}  // namespace bustub
