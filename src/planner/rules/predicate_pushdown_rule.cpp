#include "bored/planner/rules/predicate_pushdown_rule.hpp"

namespace bored::planner {

namespace {

bool projection_pruning_transform(const RuleContext&,
                                  const LogicalOperatorPtr& root,
                                  std::vector<LogicalOperatorPtr>& alternatives)
{
    if (!root || root->children().empty()) {
        return false;
    }

    const auto& child = root->children().front();
    if (!child) {
        return false;
    }

    if (root->properties().output_columns != child->properties().output_columns) {
        return false;
    }

    alternatives.push_back(child);
    return true;
}

bool filter_pushdown_transform(const RuleContext&,
                               const LogicalOperatorPtr& root,
                               std::vector<LogicalOperatorPtr>& alternatives)
{
    if (!root || root->children().empty()) {
        return false;
    }

    const auto& projection = root->children().front();
    if (!projection || projection->children().empty()) {
        return false;
    }

    if (projection->properties().output_columns != root->properties().output_columns) {
        return false;
    }

    const auto& target = projection->children().front();
    if (!target) {
        return false;
    }

    auto pushed_filter = LogicalOperator::make(LogicalOperatorType::Filter, {target}, root->properties());
    auto new_projection = LogicalOperator::make(LogicalOperatorType::Projection, {pushed_filter}, projection->properties());

    alternatives.push_back(new_projection);
    return true;
}

bool constant_folding_transform(const RuleContext&,
                                const LogicalOperatorPtr&,
                                std::vector<LogicalOperatorPtr>&)
{
    return false;
}

}  // namespace

std::shared_ptr<Rule> make_projection_pruning_rule()
{
    return std::make_shared<Rule>(
        "ProjectionPruning",
        std::vector<LogicalOperatorType>{LogicalOperatorType::Projection},
        RuleCategory::Projection,
        projection_pruning_transform,
        /*priority=*/10);
}

std::shared_ptr<Rule> make_filter_pushdown_rule()
{
    return std::make_shared<Rule>(
        "FilterPushdown",
        std::vector<LogicalOperatorType>{LogicalOperatorType::Filter, LogicalOperatorType::Projection},
        RuleCategory::Filter,
        filter_pushdown_transform,
        /*priority=*/5);
}

std::shared_ptr<Rule> make_constant_folding_rule()
{
    return std::make_shared<Rule>(
        "ConstantFolding",
        std::vector<LogicalOperatorType>{LogicalOperatorType::Projection},
        RuleCategory::ConstantFolding,
        constant_folding_transform,
        /*priority=*/1);
}

}  // namespace bored::planner
