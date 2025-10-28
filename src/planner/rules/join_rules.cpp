#include "bored/planner/rules/join_rules.hpp"

namespace bored::planner {

namespace {

bool join_commutativity_transform(const RuleContext&,
                                  const LogicalOperatorPtr& root,
                                  std::vector<LogicalOperatorPtr>& alternatives)
{
    if (!root) {
        return false;
    }
    const auto& children = root->children();
    if (children.size() != 2U) {
        return false;
    }
    const auto& left = children[0];
    const auto& right = children[1];
    if (!left || !right) {
        return false;
    }
                                if (root->type() != LogicalOperatorType::Join) {
                                    return false;
                                }

    std::vector<LogicalOperatorPtr> swapped_children{right, left};
    auto swapped = LogicalOperator::make(LogicalOperatorType::Join, std::move(swapped_children), root->properties());
    alternatives.push_back(std::move(swapped));
    return true;
}

bool join_associativity_transform(const RuleContext&,
                                  const LogicalOperatorPtr& root,
                                  std::vector<LogicalOperatorPtr>& alternatives)
{
    if (!root || root->type() != LogicalOperatorType::Join) {
        return false;
    }

    const auto& children = root->children();
    if (children.size() != 2U) {
        return false;
    }

    const auto& left = children[0];
    const auto& right = children[1];
    if (!left || !right || left->type() != LogicalOperatorType::Join) {
        return false;
    }

    const auto& left_left = left->children();
    if (left_left.size() != 2U) {
        return false;
    }

    const auto& a = left_left[0];
    const auto& b = left_left[1];
    if (!a || !b) {
        return false;
    }

    // Build (A ⋈ (B ⋈ C)) from ((A ⋈ B) ⋈ C).
    auto rotated_right = LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{b, right},
        root->properties());

    auto rotated_root = LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{a, rotated_right},
        root->properties());

    alternatives.push_back(std::move(rotated_root));
    return true;
}

}  // namespace

std::shared_ptr<Rule> make_join_commutativity_rule()
{
    return std::make_shared<Rule>(
        "JoinCommutativity",
        std::vector<LogicalOperatorType>{LogicalOperatorType::Join},
        RuleCategory::Generic,
        join_commutativity_transform,
        /*priority=*/4);
}

std::shared_ptr<Rule> make_join_associativity_rule()
{
    return std::make_shared<Rule>(
        "JoinAssociativity",
        std::vector<LogicalOperatorType>{LogicalOperatorType::Join, LogicalOperatorType::Join},
        RuleCategory::Generic,
        join_associativity_transform,
        /*priority=*/3);
}

}  // namespace bored::planner
