#include "bored/planner/rules/join_rules.hpp"

#include "bored/planner/planner_context.hpp"
#include "bored/planner/statistics_catalog.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace bored::planner {

namespace {

struct JoinLeaf final {
    LogicalOperatorPtr node;
    double rows = 1.0;
};

void append_unique(std::vector<std::string>& target, const std::vector<std::string>& values)
{
    for (const auto& value : values) {
        if (std::find(target.begin(), target.end(), value) == target.end()) {
            target.push_back(value);
        }
    }
}

double estimate_rows(const LogicalOperatorPtr& node, const PlannerContext* planner_context)
{
    if (!node) {
        return 1.0;
    }

    const auto& props = node->properties();
    if (props.estimated_cardinality > 0U) {
        return std::max(1.0, static_cast<double>(props.estimated_cardinality));
    }

    if (planner_context != nullptr) {
        const auto* statistics = planner_context->statistics();
        if (statistics != nullptr && !props.relation_name.empty()) {
            if (const auto* table = statistics->find_table(props.relation_name)) {
                const auto rows = table->row_count();
                if (rows > 0.0) {
                    return rows;
                }
            }
        }
    }

    return 1.0;
}

void collect_join_leaves(const LogicalOperatorPtr& node, std::vector<LogicalOperatorPtr>& leaves)
{
    if (!node) {
        return;
    }
    if (node->type() != LogicalOperatorType::Join) {
        leaves.push_back(node);
        return;
    }

    for (const auto& child : node->children()) {
        collect_join_leaves(child, leaves);
    }
}

LogicalProperties derive_join_properties(const LogicalOperatorPtr& left,
                                         const LogicalOperatorPtr& right,
                                         const LogicalProperties& template_props,
                                         double join_rows)
{
    LogicalProperties properties = template_props;
    properties.output_columns.clear();
    append_unique(properties.output_columns, left->properties().output_columns);
    append_unique(properties.output_columns, right->properties().output_columns);
    properties.estimated_cardinality = static_cast<std::size_t>(std::max(1.0, join_rows));
    properties.preserves_order = false;
    properties.requires_recursive_cursor = template_props.requires_recursive_cursor ||
                                           left->properties().requires_recursive_cursor ||
                                           right->properties().requires_recursive_cursor;
    properties.available_indexes.clear();
    properties.equality_predicates.clear();
    return properties;
}

JoinLeaf combine_leaves(const JoinLeaf& left,
                        const JoinLeaf& right,
                        const LogicalProperties& template_props)
{
    const double join_rows = std::max(1.0, std::min(left.rows, right.rows));
    auto join = LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{left.node, right.node},
        derive_join_properties(left.node, right.node, template_props, join_rows));
    return {join, join_rows};
}

LogicalOperatorPtr build_greedy_join_tree(std::vector<JoinLeaf> leaves,
                                          const LogicalProperties& template_props)
{
    while (leaves.size() > 1U) {
        std::sort(leaves.begin(), leaves.end(), [](const JoinLeaf& lhs, const JoinLeaf& rhs) {
            if (lhs.rows == rhs.rows) {
                return lhs.node.get() < rhs.node.get();
            }
            return lhs.rows < rhs.rows;
        });

        auto left = leaves.front();
        leaves.erase(leaves.begin());
        auto right = leaves.front();
        leaves.erase(leaves.begin());

        leaves.push_back(combine_leaves(left, right, template_props));
    }

    return leaves.front().node;
}

bool join_commutativity_transform(const RuleContext&,
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
    if (!left || !right) {
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

bool join_greedy_reorder_transform(const RuleContext& context,
                                   const LogicalOperatorPtr& root,
                                   std::vector<LogicalOperatorPtr>& alternatives)
{
    if (!root || root->type() != LogicalOperatorType::Join) {
        return false;
    }

    std::vector<LogicalOperatorPtr> leaves;
    collect_join_leaves(root, leaves);
    if (leaves.size() < 3U) {
        return false;
    }

    const PlannerContext* planner_context = context.planner_context();
    std::vector<JoinLeaf> leaf_infos;
    leaf_infos.reserve(leaves.size());
    for (const auto& leaf : leaves) {
        leaf_infos.push_back({leaf, estimate_rows(leaf, planner_context)});
    }

    bool non_decreasing = true;
    for (std::size_t index = 1U; index < leaf_infos.size(); ++index) {
        if (leaf_infos[index - 1].rows > leaf_infos[index].rows) {
            non_decreasing = false;
            break;
        }
    }
    if (non_decreasing) {
        return false;
    }

    auto sorted_leaves = leaf_infos;
    std::sort(sorted_leaves.begin(), sorted_leaves.end(), [](const JoinLeaf& lhs, const JoinLeaf& rhs) {
        if (lhs.rows == rhs.rows) {
            return lhs.node.get() < rhs.node.get();
        }
        return lhs.rows < rhs.rows;
    });

    auto greedy_root = build_greedy_join_tree(std::move(sorted_leaves), root->properties());
    const auto& greedy_children = greedy_root->children();
    if (greedy_children.size() != 2U) {
        return false;
    }

    std::vector<LogicalOperatorPtr> top_children{greedy_children.begin(), greedy_children.end()};
    auto reordered = LogicalOperator::make(LogicalOperatorType::Join, std::move(top_children), root->properties());
    alternatives.push_back(std::move(reordered));
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

std::shared_ptr<Rule> make_join_greedy_reorder_rule()
{
    return std::make_shared<Rule>(
        "JoinGreedyReorder",
        std::vector<LogicalOperatorType>{LogicalOperatorType::Join, LogicalOperatorType::Join},
        RuleCategory::Generic,
        join_greedy_reorder_transform,
        /*priority=*/2);
}

}  // namespace bored::planner
