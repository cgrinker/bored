#include "bored/planner/cost_model.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <string_view>
#include <unordered_map>

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
constexpr double kIndexSeekIoOverhead = 0.2;
constexpr double kIndexSeekCpuOverhead = 0.00075;
constexpr double kIndexProbeIoPerTuple = 0.05;
constexpr double kIndexProbeCpuPerTuple = 0.002;

std::size_t recommend_batch_size(double rows)
{
    if (rows <= 0.0) {
        return 1U;
    }
    const auto recommended = std::clamp(rows / kDefaultBatchDivisor, 1.0, kMaxBatchSize);
    return static_cast<std::size_t>(recommended);
}

std::string normalize_identifier(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (unsigned char ch : text) {
        result.push_back(static_cast<char>(std::tolower(ch)));
    }
    return result;
}

double predicate_selectivity(const LogicalProperties& properties,
                             const StatisticsCatalog* statistics,
                             double table_rows)
{
    if (properties.equality_predicates.empty()) {
        return 1.0;
    }

    const double min_selectivity = 1.0 / std::max(1.0, table_rows);
    double selectivity = 1.0;

    for (const auto& predicate : properties.equality_predicates) {
        double column_selectivity = min_selectivity;
        if (statistics != nullptr && !properties.relation_name.empty()) {
            if (const auto* column_stats = statistics->find_column(properties.relation_name, predicate.column)) {
                if (column_stats->distinct_count.has_value() && *column_stats->distinct_count > 0.0) {
                    column_selectivity = 1.0 / std::max(1.0, *column_stats->distinct_count);
                }
            }
        }
        selectivity *= column_selectivity;
    }

    return std::clamp(selectivity, min_selectivity, 1.0);
}

struct IndexPlanCandidate final {
    PlanCost cost{};
    double output_rows = 0.0;
};

std::optional<IndexPlanCandidate> estimate_index_plan(const LogicalProperties& properties,
                                                      const StatisticsCatalog* statistics,
                                                      double table_rows)
{
    if (properties.available_indexes.empty() || properties.equality_predicates.empty()) {
        return std::nullopt;
    }

    std::unordered_map<std::string, bool> predicate_lookup;
    predicate_lookup.reserve(properties.equality_predicates.size());
    for (const auto& predicate : properties.equality_predicates) {
        predicate_lookup.emplace(normalize_identifier(predicate.column), true);
    }

    std::optional<IndexPlanCandidate> best_plan;
    double best_cost = std::numeric_limits<double>::infinity();

    for (const auto& binding : properties.available_indexes) {
        if (!binding.index_id.is_valid() || binding.key_columns.empty()) {
            continue;
        }

        bool missing_key = false;
        for (const auto& key : binding.key_columns) {
            const auto normalized = normalize_identifier(key);
            if (predicate_lookup.find(normalized) == predicate_lookup.end()) {
                missing_key = true;
                break;
            }
        }

        if (missing_key) {
            continue;
        }

        const double min_selectivity = 1.0 / std::max(1.0, table_rows);
        double selectivity = binding.unique ? min_selectivity : 1.0;

        if (!binding.unique) {
            for (const auto& key : binding.key_columns) {
                double column_selectivity = min_selectivity;
                if (statistics != nullptr && !properties.relation_name.empty()) {
                    if (const auto* column_stats = statistics->find_column(properties.relation_name, key)) {
                        if (column_stats->distinct_count.has_value() && *column_stats->distinct_count > 0.0) {
                            column_selectivity = 1.0 / std::max(1.0, *column_stats->distinct_count);
                        }
                    }
                }
                selectivity *= column_selectivity;
            }
        }

        selectivity = std::clamp(selectivity, min_selectivity, 1.0);
        const double estimated_rows = std::max(1.0, table_rows * selectivity);

        PlanCost index_cost{};
        index_cost.io = kIndexSeekIoOverhead + (estimated_rows * kIndexProbeIoPerTuple);
        index_cost.cpu = kIndexSeekCpuOverhead + (estimated_rows * kIndexProbeCpuPerTuple);

        const double total_cost = index_cost.total();
        if (!best_plan.has_value() || total_cost < best_cost) {
            best_cost = total_cost;
            best_plan = IndexPlanCandidate{index_cost, estimated_rows};
        }
    }

    return best_plan;
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
        const auto selectivity = predicate_selectivity(node->properties(), statistics_, rows);
        const auto filtered_rows = std::max(1.0, rows * selectivity);

        PlanCost cost;
        cost.io = rows * kScanIoPerTuple;
        cost.cpu = rows * kScanCpuPerTuple;

        auto best_cost = cost;
        auto best_rows = filtered_rows;

        if (auto index_plan = estimate_index_plan(node->properties(), statistics_, rows)) {
            if (index_plan->cost.total() < best_cost.total()) {
                best_cost = index_plan->cost;
                best_rows = std::max(1.0, index_plan->output_rows);
            }
        }

        CostEstimate estimate{};
        estimate.cost = best_cost;
        estimate.output_rows = best_rows;
        estimate.recommended_batch_size = recommend_batch_size(best_rows);
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
