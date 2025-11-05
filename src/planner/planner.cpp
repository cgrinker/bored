#include "bored/planner/planner.hpp"
#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_relations.hpp"
#include "bored/planner/cost_model.hpp"
#include "bored/planner/memo.hpp"
#include "bored/planner/rule.hpp"
#include "bored/planner/rules/join_rules.hpp"
#include "bored/planner/rules/predicate_pushdown_rule.hpp"
#include "bored/planner/planner_telemetry.hpp"

#include <algorithm>
#include <cctype>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace bored::planner {
namespace {

constexpr std::size_t kHashJoinThreshold = 500U;
constexpr std::size_t kDefaultBatchSize = 128U;
constexpr std::size_t kMaxBatchSize = 1024U;

std::size_t default_batch_size(std::size_t cardinality) noexcept
{
    if (cardinality == 0U) {
        return kDefaultBatchSize;
    }
    const auto quarter = cardinality / 4U;
    if (quarter == 0U) {
        return 1U;
    }
    return std::clamp<std::size_t>(quarter, 1U, kMaxBatchSize);
}

void append_unique(std::vector<std::string>& target, const std::vector<std::string>& values)
{
    for (const auto& value : values) {
        if (std::find(target.begin(), target.end(), value) == target.end()) {
            target.push_back(value);
        }
    }
}

std::string_view trim_view(std::string_view text) noexcept
{
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front()))) {
        text.remove_prefix(1U);
    }
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back()))) {
        text.remove_suffix(1U);
    }
    return text;
}

std::vector<std::string> split_columns(std::string_view text)
{
    std::vector<std::string> columns;
    text = trim_view(text);
    if (text.empty()) {
        return columns;
    }

    std::size_t offset = 0U;
    while (offset <= text.size()) {
        const auto next = text.find(',', offset);
        const auto length = (next == std::string_view::npos) ? (text.size() - offset) : (next - offset);
        auto piece = trim_view(text.substr(offset, length));
        if (!piece.empty()) {
            columns.emplace_back(piece);
        }
        if (next == std::string_view::npos) {
            break;
        }
        offset = next + 1U;
    }
    return columns;
}

void inherit_child_shape(const PhysicalOperatorPtr& child,
                         const LogicalProperties& logical_props,
                         PhysicalProperties& properties)
{
    if (child) {
        const auto& child_props = child->properties();
        if (properties.output_columns.empty()) {
            properties.output_columns = child_props.output_columns;
        }
        if (properties.ordering_columns.empty()) {
            properties.ordering_columns = child_props.ordering_columns;
        }
        if (properties.partitioning_columns.empty()) {
            properties.partitioning_columns = child_props.partitioning_columns;
        }
        if (properties.expected_cardinality == 0U) {
            properties.expected_cardinality = child_props.expected_cardinality;
        }
        if (properties.expected_batch_size == 0U) {
            properties.expected_batch_size = child_props.expected_batch_size;
        }
        properties.preserves_order = child_props.preserves_order;
    } else {
        if (properties.output_columns.empty()) {
            properties.output_columns = logical_props.output_columns;
        }
        if (properties.expected_cardinality == 0U) {
            properties.expected_cardinality = logical_props.estimated_cardinality;
        }
        if (properties.expected_batch_size == 0U) {
            properties.expected_batch_size = default_batch_size(properties.expected_cardinality);
        }
    }
}

PhysicalOperatorPtr make_unique_enforcement(const PlannerContext& context,
                                            const LogicalProperties& logical_props,
                                            const catalog::CatalogConstraintDescriptor& descriptor,
                                            PhysicalOperatorPtr child)
{
    PhysicalProperties properties{};
    properties.relation_name = logical_props.relation_name;
    properties.relation_id = logical_props.relation_id;
    inherit_child_shape(child, logical_props, properties);
    if (properties.expected_batch_size == 0U) {
        properties.expected_batch_size = default_batch_size(properties.expected_cardinality);
    }

    UniqueEnforcementProperties unique_props{};
    unique_props.constraint_id = descriptor.constraint_id;
    unique_props.relation_id = descriptor.relation_id;
    unique_props.backing_index_id = descriptor.backing_index_id;
    unique_props.constraint_name = std::string(descriptor.name);
    unique_props.key_columns = split_columns(descriptor.key_columns);
    unique_props.is_primary_key = descriptor.constraint_type == catalog::CatalogConstraintType::PrimaryKey;
    properties.unique_enforcement = std::move(unique_props);
    properties.executor_operator_id = context.allocate_executor_operator_id();

    std::vector<PhysicalOperatorPtr> children;
    if (child) {
        children.push_back(std::move(child));
    }
    return PhysicalOperator::make(PhysicalOperatorType::UniqueEnforce, std::move(children), std::move(properties));
}

PhysicalOperatorPtr make_foreign_key_enforcement(const PlannerContext& context,
                                                 const LogicalProperties& logical_props,
                                                 const catalog::CatalogConstraintDescriptor& descriptor,
                                                 PhysicalOperatorPtr child)
{
    PhysicalProperties properties{};
    properties.relation_name = logical_props.relation_name;
    properties.relation_id = logical_props.relation_id;
    inherit_child_shape(child, logical_props, properties);
    if (properties.expected_batch_size == 0U) {
        properties.expected_batch_size = default_batch_size(properties.expected_cardinality);
    }

    ForeignKeyEnforcementProperties fk_props{};
    fk_props.constraint_id = descriptor.constraint_id;
    fk_props.relation_id = descriptor.relation_id;
    fk_props.referenced_relation_id = descriptor.referenced_relation_id;
    fk_props.backing_index_id = descriptor.backing_index_id;
    fk_props.constraint_name = std::string(descriptor.name);
    fk_props.referencing_columns = split_columns(descriptor.key_columns);
    fk_props.referenced_columns = split_columns(descriptor.referenced_columns);
    properties.foreign_key_enforcement = std::move(fk_props);
    properties.executor_operator_id = context.allocate_executor_operator_id();

    std::vector<PhysicalOperatorPtr> children;
    if (child) {
        children.push_back(std::move(child));
    }
    return PhysicalOperator::make(PhysicalOperatorType::ForeignKeyCheck, std::move(children), std::move(properties));
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
    case LogicalOperatorType::Materialize:
        return PhysicalOperatorType::Materialize;
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

    const auto* cost_model = context.cost_model();

    std::vector<PhysicalOperatorPtr> lowered_children;
    const auto& logical_children = logical->children();
    lowered_children.reserve(logical_children.size());
    for (const auto& child : logical_children) {
        lowered_children.push_back(lower_placeholder(context, child));
    }

    auto operator_type = to_physical(logical->type());

    PhysicalProperties properties{};
    properties.expected_cardinality = logical->properties().estimated_cardinality;
    properties.preserves_order = logical->properties().preserves_order;
    properties.relation_name = logical->properties().relation_name;
    properties.relation_id = logical->properties().relation_id;
    properties.output_columns = logical->properties().output_columns;
    if (cost_model != nullptr) {
        const auto estimate = cost_model->estimate_plan(logical);
        if (properties.expected_cardinality == 0U && estimate.output_rows > 0.0) {
            properties.expected_cardinality = static_cast<std::size_t>(std::max(1.0, estimate.output_rows));
        }
        if (estimate.recommended_batch_size != 0U) {
            properties.expected_batch_size = estimate.recommended_batch_size;
        }
    }
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

        auto left_cardinality = lowered_children[0]->properties().expected_cardinality;
        auto right_cardinality = lowered_children[1]->properties().expected_cardinality;
        if (cost_model != nullptr) {
            if (left_cardinality == 0U && !logical_children.empty()) {
                const auto estimate = cost_model->estimate_plan(logical_children[0]);
                if (estimate.output_rows > 0.0) {
                    left_cardinality = static_cast<std::size_t>(std::max(1.0, estimate.output_rows));
                }
            }
            if (right_cardinality == 0U && logical_children.size() >= 2U) {
                const auto estimate = cost_model->estimate_plan(logical_children[1]);
                if (estimate.output_rows > 0.0) {
                    right_cardinality = static_cast<std::size_t>(std::max(1.0, estimate.output_rows));
                }
            }
        }

        if (left_cardinality >= kHashJoinThreshold && right_cardinality >= kHashJoinThreshold) {
            operator_type = PhysicalOperatorType::HashJoin;
            properties.executor_strategy = "hash(build=left, probe=right)";
        } else {
            operator_type = PhysicalOperatorType::NestedLoopJoin;
            properties.executor_strategy = "nested-loop(probe=right)";
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

    if (logical->type() == LogicalOperatorType::Materialize && !lowered_children.empty()) {
        inherit_child_shape(lowered_children.front(), logical->properties(), properties);
        if (properties.executor_strategy.empty()) {
            properties.executor_strategy = "materialize(spool)";
        }
        MaterializeProperties materialize_props{};
        materialize_props.worktable_id = context.allocate_worktable_id();
        materialize_props.enable_recursive_cursor = logical->properties().requires_recursive_cursor;
        properties.materialize = materialize_props;
    }

    const auto apply_constraint_enforcement = [&](std::vector<PhysicalOperatorPtr>& children) {
        if (children.empty()) {
            return;
        }
        const auto relation_id = logical->properties().relation_id;
        if (!relation_id.is_valid()) {
            return;
        }
        const auto* accessor = context.catalog();
        if (accessor == nullptr) {
            return;
        }

        auto constraints = accessor->constraints(relation_id);
        if (constraints.empty()) {
            return;
        }

        std::vector<catalog::CatalogConstraintDescriptor> unique_constraints;
        std::vector<catalog::CatalogConstraintDescriptor> foreign_key_constraints;
        unique_constraints.reserve(constraints.size());
        foreign_key_constraints.reserve(constraints.size());

        for (const auto& constraint : constraints) {
            switch (constraint.constraint_type) {
            case catalog::CatalogConstraintType::PrimaryKey:
            case catalog::CatalogConstraintType::Unique:
                unique_constraints.push_back(constraint);
                break;
            case catalog::CatalogConstraintType::ForeignKey:
                foreign_key_constraints.push_back(constraint);
                break;
            default:
                break;
            }
        }

        if (unique_constraints.empty() && foreign_key_constraints.empty()) {
            return;
        }

        const auto constraint_sort = [](const catalog::CatalogConstraintDescriptor& lhs,
                                        const catalog::CatalogConstraintDescriptor& rhs) {
            return lhs.constraint_id.value < rhs.constraint_id.value;
        };
        std::sort(unique_constraints.begin(), unique_constraints.end(), constraint_sort);
        std::sort(foreign_key_constraints.begin(), foreign_key_constraints.end(), constraint_sort);

        auto chain = children.front();
        for (const auto& fk : foreign_key_constraints) {
            chain = make_foreign_key_enforcement(context, logical->properties(), fk, std::move(chain));
        }
        for (const auto& unique : unique_constraints) {
            chain = make_unique_enforcement(context, logical->properties(), unique, std::move(chain));
        }
        children.front() = std::move(chain);
    };

    properties.executor_operator_id = context.allocate_executor_operator_id();
    if (properties.expected_batch_size == 0U) {
        properties.expected_batch_size = default_batch_size(properties.expected_cardinality);
    }

    if (logical->type() == LogicalOperatorType::Insert || logical->type() == LogicalOperatorType::Update) {
        apply_constraint_enforcement(lowered_children);
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
