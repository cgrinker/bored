#pragma once

#include "bored/planner/memo.hpp"
#include "bored/planner/plan_diagnostics.hpp"
#include "bored/planner/planner_context.hpp"
#include "bored/planner/rule.hpp"

namespace bored::planner {
class CostModel;
}

namespace bored::planner::detail {

LogicalOperatorPtr explore_memo(const PlannerContext& context,
                                const RuleEngine& engine,
                                Memo& memo,
                                Memo::GroupId root_group,
                                RuleTrace* trace,
                                const CostModel* cost_model,
                                PlanDiagnostics* diagnostics);

}  // namespace bored::planner::detail
