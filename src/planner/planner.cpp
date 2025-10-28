#include "bored/planner/planner.hpp"
#include "bored/planner/memo.hpp"
#include "bored/planner/rule.hpp"
#include "bored/planner/rules/join_rules.hpp"
#include "bored/planner/rules/predicate_pushdown_rule.hpp"

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace bored::planner {
namespace {

PhysicalOperatorType to_physical(LogicalOperatorType type) noexcept
{
    switch (type) {
    case LogicalOperatorType::Projection:
        return PhysicalOperatorType::Projection;
    case LogicalOperatorType::Filter:
        return PhysicalOperatorType::Filter;
    case LogicalOperatorType::Join:
        return PhysicalOperatorType::NestedLoopJoin;
    case LogicalOperatorType::TableScan:
        return PhysicalOperatorType::SeqScan;
    case LogicalOperatorType::Values:
        return PhysicalOperatorType::Values;
    case LogicalOperatorType::Invalid:
    default:
        return PhysicalOperatorType::NoOp;
    }
}

PhysicalOperatorPtr lower_placeholder(const LogicalOperatorPtr& logical)
{
    if (!logical) {
        return PhysicalOperator::make(PhysicalOperatorType::NoOp);
    }

    std::vector<PhysicalOperatorPtr> lowered_children;
    lowered_children.reserve(logical->children().size());
    for (const auto& child : logical->children()) {
        lowered_children.push_back(lower_placeholder(child));
    }

    PhysicalProperties properties{};
    properties.expected_cardinality = logical->properties().estimated_cardinality;
    properties.preserves_order = logical->properties().preserves_order;
    properties.output_columns = logical->properties().output_columns;

    return PhysicalOperator::make(to_physical(logical->type()), std::move(lowered_children), std::move(properties));
}

const RuleRegistry& default_rule_registry()
{
    static RuleRegistry registry = [] {
        RuleRegistry reg;
        reg.register_rule(make_projection_pruning_rule());
        reg.register_rule(make_filter_pushdown_rule());
        reg.register_rule(make_constant_folding_rule());
        reg.register_rule(make_join_commutativity_rule());
        reg.register_rule(make_join_associativity_rule());
        return reg;
    }();
    return registry;
}

LogicalOperatorPtr explore_memo(const PlannerContext& context,
                                const RuleEngine& engine,
                                Memo& memo,
                                Memo::GroupId root_group,
                                RuleTrace* trace)
{
    const auto* root_group_ptr = memo.find_group(root_group);
    if (!root_group_ptr) {
        return nullptr;
    }

    std::unordered_map<const LogicalOperator*, Memo::GroupId> group_lookup;
    std::vector<std::pair<Memo::GroupId, LogicalOperatorPtr>> stack;
    stack.reserve(16U);

    for (const auto& expression : root_group_ptr->expressions()) {
        if (!expression) {
            continue;
        }
        group_lookup.emplace(expression.get(), root_group);
        stack.emplace_back(root_group, expression);
    }

    std::unordered_set<const LogicalOperator*> visited;
    LogicalOperatorPtr chosen_root_alternative;

    const auto ensure_group = [&](const LogicalOperatorPtr& expression) {
        if (!expression) {
            return Memo::invalid_group();
        }
        const auto raw = expression.get();
        if (auto it = group_lookup.find(raw); it != group_lookup.end()) {
            return it->second;
        }
        auto group_id = memo.add_group(expression);
        group_lookup.emplace(raw, group_id);
        return group_id;
    };

    while (!stack.empty()) {
        auto [group_id, expression] = stack.back();
        stack.pop_back();
        if (!expression) {
            continue;
        }

        const auto raw = expression.get();
        if (!visited.insert(raw).second) {
            continue;
        }

        RuleContext rule_context{&context, &memo, group_id};
        std::vector<LogicalOperatorPtr> alternatives;
        engine.apply_rules(rule_context, expression, alternatives, trace);

        if (!chosen_root_alternative && group_id == root_group && !alternatives.empty()) {
            chosen_root_alternative = alternatives.front();
        }

        const auto* group_ptr = memo.find_group(group_id);
        if (!group_ptr) {
            continue;
        }

        for (const auto& member : group_ptr->expressions()) {
            if (!member) {
                continue;
            }

            const auto member_raw = member.get();
            if (!group_lookup.contains(member_raw)) {
                group_lookup.emplace(member_raw, group_id);
            }

            if (!visited.count(member_raw)) {
                stack.emplace_back(group_id, member);
            }

            for (const auto& child : member->children()) {
                if (!child) {
                    continue;
                }
                const auto child_group_id = ensure_group(child);
                if (!visited.count(child.get())) {
                    stack.emplace_back(child_group_id, child);
                }
            }
        }
    }

    if (chosen_root_alternative) {
        return chosen_root_alternative;
    }

    const auto* final_group = memo.find_group(root_group);
    if (!final_group || final_group->expressions().empty()) {
        return nullptr;
    }

    return final_group->expressions().front();
}

}  // namespace

PlannerResult plan_query(const PlannerContext& context, const LogicalPlan& plan)
{
    PlannerResult result{};
    auto root = plan.root();
    if (!root) {
        result.diagnostics.emplace_back("logical plan is empty");
        return result;
    }

    Memo memo;
    auto root_group = memo.add_group(root);

    RuleEngine engine{&default_rule_registry(), context.options().rule_options};
    RuleTrace trace{};
    auto* trace_ptr = context.options().enable_rule_tracing ? &trace : nullptr;
    if (auto representative = explore_memo(context, engine, memo, root_group, trace_ptr)) {
        root = std::move(representative);
    }

    result.plan = PhysicalPlan{lower_placeholder(root)};
    if (trace_ptr) {
        for (const auto& application : trace_ptr->applications) {
            result.diagnostics.push_back(application.rule_name + (application.success ? ":applied" : ":skipped"));
        }
    }
    return result;
}

}  // namespace bored::planner
