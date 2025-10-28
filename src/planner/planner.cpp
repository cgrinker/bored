#include "bored/planner/planner.hpp"
#include "bored/planner/cost_model.hpp"
#include "bored/planner/memo.hpp"
#include "bored/planner/rule.hpp"
#include "bored/planner/rules/join_rules.hpp"
#include "bored/planner/rules/predicate_pushdown_rule.hpp"
#include "bored/planner/planner_telemetry.hpp"

#include <algorithm>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace bored::planner {
namespace {

constexpr std::size_t kHashJoinThreshold = 500U;

void append_unique(std::vector<std::string>& target, const std::vector<std::string>& values)
{
    for (const auto& value : values) {
        if (std::find(target.begin(), target.end(), value) == target.end()) {
            target.push_back(value);
        }
    }
}

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
    case LogicalOperatorType::Insert:
        return PhysicalOperatorType::Insert;
    case LogicalOperatorType::Update:
        return PhysicalOperatorType::Update;
    case LogicalOperatorType::Delete:
        return PhysicalOperatorType::Delete;
    case LogicalOperatorType::Invalid:
    default:
        return PhysicalOperatorType::NoOp;
    }
}

bool snapshot_requires_visibility(const txn::Snapshot& snapshot) noexcept
{
    return snapshot.read_lsn != 0U || snapshot.xmin != 0U || snapshot.xmax != 0U || !snapshot.in_progress.empty();
}

PhysicalOperatorPtr lower_placeholder(const PlannerContext& context, const LogicalOperatorPtr& logical)
{
    if (!logical) {
        return PhysicalOperator::make(PhysicalOperatorType::NoOp);
    }

    std::vector<PhysicalOperatorPtr> lowered_children;
    lowered_children.reserve(logical->children().size());
    for (const auto& child : logical->children()) {
        lowered_children.push_back(lower_placeholder(context, child));
    }

    auto operator_type = to_physical(logical->type());

    PhysicalProperties properties{};
    properties.expected_cardinality = logical->properties().estimated_cardinality;
    properties.preserves_order = logical->properties().preserves_order;
    properties.relation_name = logical->properties().relation_name;
    properties.output_columns = logical->properties().output_columns;
    if (logical->type() == LogicalOperatorType::TableScan) {
        properties.requires_visibility_check = snapshot_requires_visibility(context.snapshot());
        if (properties.requires_visibility_check) {
            properties.snapshot = context.snapshot();
        }
        append_unique(properties.partitioning_columns, properties.output_columns);
    }

    if (properties.preserves_order) {
        properties.ordering_columns = properties.output_columns;
    }

    if (!lowered_children.empty()) {
        const auto& first_child_props = lowered_children.front()->properties();
        if (properties.ordering_columns.empty()) {
            properties.ordering_columns = first_child_props.ordering_columns;
        }
        if (properties.partitioning_columns.empty()) {
            properties.partitioning_columns = first_child_props.partitioning_columns;
        }
    }

    if (logical->type() == LogicalOperatorType::Join && lowered_children.size() == 2U) {
        properties.partitioning_columns.clear();
        append_unique(properties.partitioning_columns, lowered_children[0]->properties().partitioning_columns);
        append_unique(properties.partitioning_columns, lowered_children[1]->properties().partitioning_columns);
        if (properties.ordering_columns.empty()) {
            properties.ordering_columns = lowered_children[0]->properties().ordering_columns;
        }

        const auto left_cardinality = lowered_children[0]->properties().expected_cardinality;
        const auto right_cardinality = lowered_children[1]->properties().expected_cardinality;
        if (left_cardinality >= kHashJoinThreshold && right_cardinality >= kHashJoinThreshold) {
            operator_type = PhysicalOperatorType::HashJoin;
        } else {
            operator_type = PhysicalOperatorType::NestedLoopJoin;
        }
    }

    if (logical->type() == LogicalOperatorType::Update || logical->type() == LogicalOperatorType::Delete) {
        properties.requires_visibility_check = true;
        properties.snapshot = context.snapshot();
    }

    if (logical->type() == LogicalOperatorType::Insert) {
        operator_type = PhysicalOperatorType::Insert;
    } else if (logical->type() == LogicalOperatorType::Update) {
        operator_type = PhysicalOperatorType::Update;
    } else if (logical->type() == LogicalOperatorType::Delete) {
        operator_type = PhysicalOperatorType::Delete;
    }

    return PhysicalOperator::make(operator_type, std::move(lowered_children), std::move(properties));
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
                                RuleTrace* trace,
                                const CostModel* cost_model,
                                PlanDiagnostics* diagnostics)
{
    const auto* root_group_ptr = memo.find_group(root_group);
    if (!root_group_ptr) {
        return nullptr;
    }

    if (diagnostics) {
        diagnostics->alternatives.clear();
        diagnostics->cost_evaluations = 0U;
        diagnostics->chosen_plan_cost = 0.0;
        diagnostics->chosen_logical_plan = nullptr;
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

    const auto* final_group = memo.find_group(root_group);
    if (!final_group || final_group->expressions().empty()) {
        return nullptr;
    }

    if (!cost_model) {
        for (auto it = final_group->expressions().rbegin(); it != final_group->expressions().rend(); ++it) {
            if (*it) {
                if (diagnostics) {
                    diagnostics->chosen_logical_plan = *it;
                }
                return *it;
            }
        }
        return nullptr;
    }

    double best_cost = std::numeric_limits<double>::infinity();
    LogicalOperatorPtr best_expression;

    for (const auto& expression : final_group->expressions()) {
        if (!expression) {
            continue;
        }
        const auto estimate = cost_model->estimate_plan(expression);
        const double total_cost = estimate.cost.total();
        if (diagnostics) {
            diagnostics->alternatives.push_back({expression, total_cost});
            ++diagnostics->cost_evaluations;
        }
        if (total_cost < best_cost) {
            best_cost = total_cost;
            best_expression = expression;
        }
    }

    if (diagnostics && best_expression) {
        diagnostics->chosen_logical_plan = best_expression;
        if (best_cost < std::numeric_limits<double>::infinity()) {
            diagnostics->chosen_plan_cost = best_cost;
        }
    }

    return best_expression;
}

}  // namespace

PlannerResult plan_query(const PlannerContext& context, const LogicalPlan& plan)
{
    PlannerResult result{};
    PlannerTelemetry* telemetry = context.telemetry();
    if (telemetry != nullptr) {
        telemetry->record_plan_attempt();
    }
    auto root = plan.root();
    if (!root) {
        result.diagnostics.emplace_back("logical plan is empty");
        if (telemetry != nullptr) {
            telemetry->record_plan_failure();
        }
        return result;
    }

    Memo memo;
    auto root_group = memo.add_group(root);

    RuleEngine engine{&default_rule_registry(), context.options().rule_options};
    RuleTrace trace{};
    const auto* cost_model = context.cost_model();
    if (auto representative = explore_memo(context,
                                           engine,
                                           memo,
                                           root_group,
                                           &trace,
                                           cost_model,
                                           &result.plan_diagnostics)) {
        root = std::move(representative);
    }
    if (!result.plan_diagnostics.chosen_logical_plan) {
        result.plan_diagnostics.chosen_logical_plan = root;
    }

    result.plan = PhysicalPlan{lower_placeholder(context, root)};

    result.plan_diagnostics.rules_attempted = trace.applications.size();
    result.plan_diagnostics.rules_applied = static_cast<std::size_t>(std::count_if(
        trace.applications.begin(), trace.applications.end(), [](const auto& application) {
            return application.success;
        }));
    result.plan_diagnostics.rule_trace.clear();
    result.plan_diagnostics.rule_trace.reserve(trace.applications.size());
    for (const auto& application : trace.applications) {
        result.plan_diagnostics.rule_trace.push_back({application.rule_name, application.success});
    }

    if (context.options().enable_rule_tracing) {
        for (const auto& application : trace.applications) {
            result.diagnostics.push_back(application.rule_name + (application.success ? ":applied" : ":skipped"));
        }
    }

    if (telemetry != nullptr) {
        telemetry->record_plan_success(result.plan_diagnostics);
    }
    return result;
}

}  // namespace bored::planner
