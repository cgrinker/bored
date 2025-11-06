#include "bored/planner/plan_printer.hpp"

#include "bored/planner/physical_plan.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace bored::planner {

namespace {

std::string to_string(PhysicalOperatorType type)
{
    switch (type) {
    case PhysicalOperatorType::NoOp:
        return "NoOp";
    case PhysicalOperatorType::Projection:
        return "Projection";
    case PhysicalOperatorType::Filter:
        return "Filter";
    case PhysicalOperatorType::SeqScan:
        return "SeqScan";
    case PhysicalOperatorType::IndexScan:
        return "IndexScan";
    case PhysicalOperatorType::NestedLoopJoin:
        return "NestedLoopJoin";
    case PhysicalOperatorType::HashJoin:
        return "HashJoin";
    case PhysicalOperatorType::Values:
        return "Values";
    case PhysicalOperatorType::Materialize:
        return "Materialize";
    case PhysicalOperatorType::Insert:
        return "Insert";
    case PhysicalOperatorType::Update:
        return "Update";
    case PhysicalOperatorType::Delete:
        return "Delete";
    case PhysicalOperatorType::UniqueEnforce:
        return "UniqueEnforce";
    case PhysicalOperatorType::ForeignKeyCheck:
        return "ForeignKeyCheck";
    }
    return "Unknown";
}

std::string join(const std::vector<std::string>& values)
{
    if (values.empty()) {
        return {};
    }
    std::ostringstream oss;
    oss << "[";
    for (std::size_t i = 0U; i < values.size(); ++i) {
        if (i > 0U) {
            oss << ", ";
        }
        oss << values[i];
    }
    oss << "]";
    return oss.str();
}

std::string format_snapshot(const txn::Snapshot& snapshot)
{
    std::ostringstream oss;
    oss << "{read_lsn=" << snapshot.read_lsn
        << ", xmin=" << snapshot.xmin
        << ", xmax=" << snapshot.xmax
        << ", in_progress_count=" << snapshot.in_progress.size()
        << "}";
    return oss.str();
}

std::string format_runtime(const PhysicalProperties& props, const ExplainOptions& options)
{
    if (options.runtime_stats == nullptr || props.executor_operator_id == 0U) {
        return {};
    }

    const auto& runtime_map = *options.runtime_stats;
    const auto it = runtime_map.find(props.executor_operator_id);
    if (it == runtime_map.end()) {
        return {};
    }

    const auto& stats = it->second;
    std::string detail{"runtime={"};
    bool first = true;
    const auto append_field = [&](const char* name, std::uint64_t value) {
        if (!first) {
            detail.append(", ");
        }
        first = false;
        detail.append(name);
        detail.push_back('=');
        detail.append(std::to_string(value));
    };

    append_field("loops", stats.loops);
    append_field("rows", stats.rows);
    append_field("total_ns", stats.total_duration_ns);
    append_field("last_ns", stats.last_duration_ns);
    detail.push_back('}');
    return detail;
}

std::string describe(const PhysicalOperatorPtr& node, const ExplainOptions& options)
{
    std::string description = to_string(node->type());
    if (!options.include_properties) {
        if (options.runtime_stats == nullptr) {
            return description;
        }
        const auto runtime_detail = format_runtime(node->properties(), options);
        if (runtime_detail.empty()) {
            return description;
        }
        description += " [";
        description += runtime_detail;
        description += "]";
        return description;
    }

    const auto& props = node->properties();
    std::vector<std::string> details;
    if (props.expected_cardinality != 0U) {
        details.push_back("cardinality=" + std::to_string(props.expected_cardinality));
    }
    if (!props.relation_name.empty()) {
        details.push_back("relation=" + props.relation_name);
    }
    if (!props.output_columns.empty()) {
        details.push_back("output=" + join(props.output_columns));
    }
    if (!props.ordering_columns.empty()) {
        details.push_back("ordering=" + join(props.ordering_columns));
    } else if (props.preserves_order) {
        details.push_back("ordering=preserved");
    }
    if (!props.partitioning_columns.empty()) {
        details.push_back("partitioning=" + join(props.partitioning_columns));
    }
    if (props.requires_visibility_check) {
        details.push_back("visibility=required");
    }
    if (options.include_snapshot && props.snapshot.has_value()) {
        details.push_back("snapshot=" + format_snapshot(*props.snapshot));
    }
    if (props.expected_batch_size != 0U) {
        details.push_back("batch=" + std::to_string(props.expected_batch_size));
    }
    if (!props.executor_strategy.empty()) {
        details.push_back("strategy=" + props.executor_strategy);
    }
    if (props.index_scan.has_value()) {
        const auto& index = *props.index_scan;
        if (index.index_id.is_valid()) {
            details.push_back("index_id=" + std::to_string(index.index_id.value));
        }
        if (!index.index_name.empty()) {
            details.push_back("index=" + index.index_name);
        }
        if (!index.key_columns.empty()) {
            details.push_back("key_columns=" + join(index.key_columns));
        }
        std::ostringstream selectivity_stream;
        selectivity_stream << std::fixed << std::setprecision(4) << index.estimated_selectivity;
        details.push_back("selectivity=" + selectivity_stream.str());
        details.push_back(std::string{"heap_fallback="} + (index.enable_heap_fallback ? "true" : "false"));
    }
    if (props.unique_enforcement.has_value()) {
        const auto& unique = *props.unique_enforcement;
        if (!unique.constraint_name.empty()) {
            details.push_back("constraint=" + unique.constraint_name);
        }
        details.push_back(std::string{"type="} + (unique.is_primary_key ? "primary-key" : "unique"));
        if (!unique.key_columns.empty()) {
            details.push_back("columns=" + join(unique.key_columns));
        }
    }
    if (props.foreign_key_enforcement.has_value()) {
        const auto& fk = *props.foreign_key_enforcement;
        if (!fk.constraint_name.empty()) {
            details.push_back("constraint=" + fk.constraint_name);
        }
        details.push_back("type=foreign-key");
        if (!fk.referencing_columns.empty()) {
            details.push_back("referencing=" + join(fk.referencing_columns));
        }
        if (!fk.referenced_columns.empty()) {
            details.push_back("referenced=" + join(fk.referenced_columns));
        }
    }
    if (props.materialize.has_value()) {
        const auto& materialize = *props.materialize;
        if (materialize.worktable_id != 0U) {
            details.push_back("worktable=" + std::to_string(materialize.worktable_id));
        }
        details.push_back(std::string{"recursive_cursor="} + (materialize.enable_recursive_cursor ? "enabled" : "disabled"));
    }

    const auto runtime_detail = format_runtime(props, options);
    if (!runtime_detail.empty()) {
        details.push_back(runtime_detail);
    }

    if (details.empty()) {
        return description;
    }

    description += " [";
    for (std::size_t i = 0U; i < details.size(); ++i) {
        if (i > 0U) {
            description += ", ";
        }
        description += details[i];
    }
    description += "]";
    return description;
}

void render(const PhysicalOperatorPtr& node,
            std::size_t depth,
            const ExplainOptions& options,
            std::vector<std::string>& lines)
{
    if (!node) {
        return;
    }

    std::string line(depth * 2U, ' ');
    if (depth > 0U) {
        line += "- ";
    }
    line += describe(node, options);
    lines.push_back(std::move(line));

    const auto& children = node->children();
    for (const auto& child : children) {
        render(child, depth + 1U, options, lines);
    }
}

}  // namespace

std::string explain_plan(const PhysicalPlan& plan, ExplainOptions options)
{
    auto root = plan.root();
    if (!root) {
        return "(empty plan)";
    }

    std::vector<std::string> lines;
    lines.reserve(8U);
    render(root, 0U, options, lines);

    std::ostringstream oss;
    for (std::size_t i = 0U; i < lines.size(); ++i) {
        if (i > 0U) {
            oss << '\n';
        }
        oss << lines[i];
    }
    return oss.str();
}

}  // namespace bored::planner
