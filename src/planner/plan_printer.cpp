#include "bored/planner/plan_printer.hpp"

#include "bored/planner/physical_plan.hpp"

#include <algorithm>
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
    case PhysicalOperatorType::NestedLoopJoin:
        return "NestedLoopJoin";
    case PhysicalOperatorType::HashJoin:
        return "HashJoin";
    case PhysicalOperatorType::Values:
        return "Values";
    case PhysicalOperatorType::Insert:
        return "Insert";
    case PhysicalOperatorType::Update:
        return "Update";
    case PhysicalOperatorType::Delete:
        return "Delete";
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

std::string describe(const PhysicalOperatorPtr& node, const ExplainOptions& options)
{
    std::string description = to_string(node->type());
    if (!options.include_properties) {
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
