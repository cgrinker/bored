#include "bored/planner/cost_model.hpp"

#include <algorithm>

namespace bored::planner {
namespace {

constexpr double kScanIoPerTuple = 0.01;
constexpr double kScanCpuPerTuple = 0.001;
constexpr double kFilterCpuPerTuple = 0.0005;
constexpr double kProjectionCpuPerTuple = 0.00025;
constexpr double kJoinCpuPerPair = 0.00001;
constexpr double kJoinIoPerInputTuple = 0.0025;
constexpr double kDefaultBatchDivisor = 4.0;
constexpr double kMaxBatchSize = 1024.0;
constexpr double kMaterializeIoPerTuple = 0.0035;
constexpr double kMaterializeCpuPerTuple = 0.00075;

std::size_t recommend_batch_size(double rows)
{
    if (rows <= 0.0) {
        return 1U;
    }
    const auto recommended = std::clamp(rows / kDefaultBatchDivisor, 1.0, kMaxBatchSize);
    return static_cast<std::size_t>(recommended);
}

CostEstimate combine_children(const CostEstimate& left, const CostEstimate& right)
{
    CostEstimate combined;
    combined.cost.io = left.cost.io + right.cost.io;
    combined.cost.cpu = left.cost.cpu + right.cost.cpu;
    combined.output_rows = left.output_rows + right.output_rows;
    combined.recommended_batch_size = std::max(left.recommended_batch_size, right.recommended_batch_size);
    return combined;
}

}  // namespace

CostModel::CostModel(const StatisticsCatalog* statistics) noexcept
    : statistics_{statistics}
{
}

CostEstimate CostModel::estimate_plan(const LogicalOperatorPtr& root) const
{
    return estimate_node(root);
}

CostEstimate CostModel::estimate_node(const LogicalOperatorPtr& node) const
{
    if (!node) {
        return {};
    }

    switch (node->type()) {
    case LogicalOperatorType::TableScan: {
        const auto rows = lookup_table_rows(node->properties());
        PlanCost cost;
        cost.io = rows * kScanIoPerTuple;
        cost.cpu = rows * kScanCpuPerTuple;
        CostEstimate estimate{};
        estimate.cost = cost;
        estimate.output_rows = rows;
        estimate.recommended_batch_size = recommend_batch_size(rows);
        return estimate;
    }
    case LogicalOperatorType::Filter: {
        const auto& children = node->children();
        if (children.empty()) {
            return {};
        }

        auto child_estimate = estimate_node(children.front());
        child_estimate.cost.cpu += child_estimate.output_rows * kFilterCpuPerTuple;

        // Assume selectivity reduces rows by half for baseline costing.
        child_estimate.output_rows = std::max(1.0, child_estimate.output_rows * 0.5);
        child_estimate.recommended_batch_size = recommend_batch_size(child_estimate.output_rows);
        return child_estimate;
    }
    case LogicalOperatorType::Projection: {
        const auto& children = node->children();
        if (children.empty()) {
            return {};
        }

        auto child_estimate = estimate_node(children.front());
        child_estimate.cost.cpu += child_estimate.output_rows * kProjectionCpuPerTuple;
        child_estimate.recommended_batch_size = recommend_batch_size(child_estimate.output_rows);
        return child_estimate;
    }
    case LogicalOperatorType::Join: {
        const auto& children = node->children();
        if (children.size() != 2U) {
            return {};
        }

        auto left_estimate = estimate_node(children[0]);
        auto right_estimate = estimate_node(children[1]);

        PlanCost cost{};
        cost.io = left_estimate.cost.io + right_estimate.cost.io;
        cost.cpu = left_estimate.cost.cpu + right_estimate.cost.cpu;

        const auto left_rows = std::max(1.0, left_estimate.output_rows);
        const auto right_rows = std::max(1.0, right_estimate.output_rows);

        cost.io += (left_rows + right_rows) * kJoinIoPerInputTuple;
        cost.cpu += (left_rows * right_rows) * kJoinCpuPerPair;

        CostEstimate result{};
        result.cost = cost;
        // Baseline join cardinality heuristic: minimum of inputs.
        result.output_rows = std::max(1.0, std::min(left_rows, right_rows));
        result.recommended_batch_size = recommend_batch_size(result.output_rows);
        return result;
    }
    case LogicalOperatorType::Materialize: {
        const auto& children = node->children();
        if (children.empty()) {
            return {};
        }

        auto child_estimate = estimate_node(children.front());
        const auto rows = std::max(1.0, child_estimate.output_rows);
        child_estimate.cost.io += rows * kMaterializeIoPerTuple;
        child_estimate.cost.cpu += rows * kMaterializeCpuPerTuple;
        child_estimate.recommended_batch_size = recommend_batch_size(child_estimate.output_rows);
        return child_estimate;
    }
    case LogicalOperatorType::Values:
    default: {
        CostEstimate aggregate{};
        const auto& children = node->children();
        for (const auto& child : children) {
            aggregate = combine_children(aggregate, estimate_node(child));
        }

        if (node->properties().estimated_cardinality > 0U) {
            aggregate.output_rows = static_cast<double>(node->properties().estimated_cardinality);
            aggregate.recommended_batch_size = recommend_batch_size(aggregate.output_rows);
        }

        return aggregate;
    }
    }
}

double CostModel::lookup_table_rows(const LogicalProperties& properties) const noexcept
{
    if (statistics_ && !properties.relation_name.empty()) {
        if (const auto* table = statistics_->find_table(properties.relation_name)) {
            return std::max(1.0, table->row_count());
        }
    }
    if (properties.estimated_cardinality > 0U) {
        return static_cast<double>(properties.estimated_cardinality);
    }
    return 1.0;
}

}  // namespace bored::planner
