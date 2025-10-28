#include "bored/planner/cost_model.hpp"
#include "bored/planner/logical_plan.hpp"
#include "bored/planner/planner.hpp"
#include "bored/planner/planner_context.hpp"
#include "bored/planner/statistics_catalog.hpp"
#include "bored/txn/transaction_types.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using namespace bored::planner;
using bored::txn::Snapshot;

struct BenchmarkOptions final {
    std::size_t samples = 50U;
    bool json_output = false;
    std::optional<std::filesystem::path> baseline_path{};
    double tolerance = 0.10;  // 10% regression budget by default
};

struct BenchmarkResult final {
    std::string name{};
    std::vector<double> samples_ms{};
    std::size_t logical_nodes = 0U;
};

struct Summary final {
    double mean_ms = 0.0;
    double min_ms = 0.0;
    double max_ms = 0.0;
    double p95_ms = 0.0;
};

struct BaselineEntry final {
    double mean_ms = 0.0;
    double p95_ms = 0.0;
};

using BaselineMap = std::unordered_map<std::string, BaselineEntry>;

[[noreturn]] void usage()
{
    std::cerr << "Usage: bored_planner_benchmarks [options]\n"
              << "  --samples=N         Samples per benchmark (default 50)\n"
              << "  --baseline=PATH     Load baseline JSON for regression enforcement\n"
              << "  --tolerance=F       Allowable fractional regression over baseline (default 0.10)\n"
              << "  --json              Emit JSON instead of table output\n"
              << "  --help              Show this message\n";
    std::exit(1);
}

std::size_t parse_size(std::string_view value, std::string_view option)
{
    std::size_t parsed = 0U;
    const auto* begin = value.data();
    const auto* end = value.data() + value.size();
    if (auto [ptr, ec] = std::from_chars(begin, end, parsed); ec != std::errc{} || ptr != end) {
        throw std::invalid_argument(std::string{"Invalid value for "} + std::string(option));
    }
    return parsed;
}

double parse_double(std::string_view value, std::string_view option)
{
    std::string buffer(value);
    std::size_t consumed = 0U;
    double parsed = 0.0;
    try {
        parsed = std::stod(buffer, &consumed);
    } catch (const std::exception&) {
        throw std::invalid_argument(std::string{"Invalid value for "} + std::string(option));
    }
    if (consumed != buffer.size()) {
        throw std::invalid_argument(std::string{"Invalid value for "} + std::string(option));
    }
    return parsed;
}

BenchmarkOptions parse_options(int argc, char** argv)
{
    BenchmarkOptions options{};
    for (int index = 1; index < argc; ++index) {
        std::string_view argument{argv[index]};
        if (argument == "--json") {
            options.json_output = true;
        } else if (argument == "--help") {
            usage();
        } else if (argument.rfind("--samples=", 0) == 0) {
            options.samples = parse_size(argument.substr(10), "--samples");
        } else if (argument.rfind("--baseline=", 0) == 0) {
            auto path_value = argument.substr(11);
            if (path_value.empty()) {
                throw std::invalid_argument("--baseline requires a path");
            }
            options.baseline_path = std::filesystem::path(std::string(path_value));
        } else if (argument.rfind("--tolerance=", 0) == 0) {
            options.tolerance = parse_double(argument.substr(12), "--tolerance");
            if (options.tolerance < 0.0) {
                throw std::invalid_argument("--tolerance must be non-negative");
            }
        } else {
            usage();
        }
    }
    return options;
}

Summary summarise(const std::vector<double>& samples)
{
    if (samples.empty()) {
        return {};
    }

    Summary summary{};
    summary.min_ms = *std::min_element(samples.begin(), samples.end());
    summary.max_ms = *std::max_element(samples.begin(), samples.end());
    summary.mean_ms = std::accumulate(samples.begin(), samples.end(), 0.0) / static_cast<double>(samples.size());

    auto sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    const double percentile = 0.95 * static_cast<double>(sorted.size());
    const std::size_t index = percentile <= 1.0 ? 0U : static_cast<std::size_t>(std::ceil(percentile)) - 1U;
    summary.p95_ms = sorted[std::min(index, sorted.size() - 1U)];
    return summary;
}

std::size_t count_nodes(const LogicalOperatorPtr& node)
{
    if (!node) {
        return 0U;
    }
    std::size_t total = 1U;
    for (const auto& child : node->children()) {
        total += count_nodes(child);
    }
    return total;
}

LogicalOperatorPtr make_scan_operator()
{
    LogicalProperties props{};
    props.estimated_cardinality = 1000U;
    props.relation_name = "public.orders";
    props.output_columns = {"order_id", "customer_id", "status"};
    return LogicalOperator::make(LogicalOperatorType::TableScan, {}, props);
}

LogicalPlan make_simple_select_plan()
{
    auto scan = make_scan_operator();
    LogicalProperties projection_props = scan->properties();
    projection_props.output_columns = {"order_id", "status"};
    auto projection = LogicalOperator::make(LogicalOperatorType::Projection, {scan}, projection_props);
    return LogicalPlan{projection};
}

LogicalPlan make_join_plan()
{
    auto scan_orders = make_scan_operator();

    LogicalProperties customer_props{};
    customer_props.estimated_cardinality = 5000U;
    customer_props.relation_name = "public.customers";
    customer_props.output_columns = {"customer_id", "region"};
    auto scan_customers = LogicalOperator::make(LogicalOperatorType::TableScan, {}, customer_props);

    LogicalProperties region_props{};
    region_props.estimated_cardinality = 200U;
    region_props.relation_name = "public.regions";
    region_props.output_columns = {"region", "priority"};
    auto scan_regions = LogicalOperator::make(LogicalOperatorType::TableScan, {}, region_props);

    LogicalProperties join_left_props{};
    join_left_props.output_columns = {"order_id", "customer_id", "status", "region"};
    auto first_join = LogicalOperator::make(LogicalOperatorType::Join,
                                            std::vector<LogicalOperatorPtr>{scan_orders, scan_customers},
                                            join_left_props);

    LogicalProperties join_root_props{};
    join_root_props.output_columns = {"order_id", "customer_id", "status", "priority"};
    auto second_join = LogicalOperator::make(LogicalOperatorType::Join,
                                             std::vector<LogicalOperatorPtr>{first_join, scan_regions},
                                             join_root_props);
    return LogicalPlan{second_join};
}

LogicalPlan make_update_plan()
{
    auto scan = make_scan_operator();
    LogicalProperties update_props{};
    update_props.relation_name = "public.orders";
    update_props.output_columns = {"order_id", "status"};
    auto update = LogicalOperator::make(LogicalOperatorType::Update,
                                        std::vector<LogicalOperatorPtr>{scan},
                                        update_props);
    return LogicalPlan{update};
}

BenchmarkResult benchmark_select(const BenchmarkOptions& options)
{
    const auto plan = make_simple_select_plan();
    BenchmarkResult result{};
    result.name = "select_projection";
    result.logical_nodes = count_nodes(plan.root());

    for (std::size_t sample = 0; sample < options.samples; ++sample) {
        PlannerContext context{};
        const auto start = std::chrono::steady_clock::now();
        auto planner_result = plan_query(context, plan);
        const auto end = std::chrono::steady_clock::now();
        (void)planner_result;
        const auto duration = std::chrono::duration<double, std::milli>(end - start).count();
        result.samples_ms.push_back(duration);
    }

    return result;
}

BenchmarkResult benchmark_join(const BenchmarkOptions& options)
{
    static StatisticsCatalog statistics;
    static CostModel cost_model{&statistics};

    statistics.clear();

    TableStatistics orders;
    orders.set_row_count(1000.0);
    statistics.register_table("public.orders", orders);

    TableStatistics customers;
    customers.set_row_count(5000.0);
    statistics.register_table("public.customers", customers);

    TableStatistics regions;
    regions.set_row_count(200.0);
    statistics.register_table("public.regions", regions);

    const auto plan = make_join_plan();
    BenchmarkResult result{};
    result.name = "join_costing";
    result.logical_nodes = count_nodes(plan.root());

    PlannerContextConfig base_config{};
    base_config.statistics = &statistics;
    base_config.cost_model = &cost_model;

    for (std::size_t sample = 0; sample < options.samples; ++sample) {
        PlannerContext context{base_config};
        const auto start = std::chrono::steady_clock::now();
        auto planner_result = plan_query(context, plan);
        const auto end = std::chrono::steady_clock::now();
        (void)planner_result;
        const auto duration = std::chrono::duration<double, std::milli>(end - start).count();
        result.samples_ms.push_back(duration);
    }

    return result;
}

BenchmarkResult benchmark_update(const BenchmarkOptions& options)
{
    const auto plan = make_update_plan();
    BenchmarkResult result{};
    result.name = "update_snapshot";
    result.logical_nodes = count_nodes(plan.root());

    Snapshot snapshot{};
    snapshot.read_lsn = 1024U;
    snapshot.xmin = 25U;
    snapshot.xmax = 125U;

    PlannerContextConfig base_config{};
    base_config.snapshot = snapshot;

    for (std::size_t sample = 0; sample < options.samples; ++sample) {
        PlannerContext context{base_config};
        const auto start = std::chrono::steady_clock::now();
        auto planner_result = plan_query(context, plan);
        const auto end = std::chrono::steady_clock::now();
        (void)planner_result;
        const auto duration = std::chrono::duration<double, std::milli>(end - start).count();
        result.samples_ms.push_back(duration);
    }

    return result;
}

void print_json(const std::vector<BenchmarkResult>& results)
{
    std::cout << "{\"benchmarks\":[";
    for (std::size_t index = 0; index < results.size(); ++index) {
        const auto& result = results[index];
        const auto summary = summarise(result.samples_ms);
        if (index > 0U) {
            std::cout << ',';
        }
        std::cout << "{\"name\":\"" << result.name << "\""
                  << ",\"samples\":" << result.samples_ms.size()
                  << ",\"logical_nodes\":" << result.logical_nodes
                  << ",\"mean_ms\":" << std::fixed << std::setprecision(3) << summary.mean_ms
                  << ",\"min_ms\":" << std::fixed << std::setprecision(3) << summary.min_ms
                  << ",\"max_ms\":" << std::fixed << std::setprecision(3) << summary.max_ms
                  << ",\"p95_ms\":" << std::fixed << std::setprecision(3) << summary.p95_ms
                  << '}';
    }
    std::cout << "]}" << std::endl;
}

void print_table(const std::vector<BenchmarkResult>& results)
{
    std::cout << std::left << std::setw(22) << "Benchmark"
              << std::right << std::setw(10) << "Samples"
              << std::setw(14) << "Logical Nodes"
              << std::setw(14) << "Mean (ms)"
              << std::setw(14) << "Min (ms)"
              << std::setw(14) << "Max (ms)"
              << std::setw(14) << "P95 (ms)" << '\n';

    for (const auto& result : results) {
        const auto summary = summarise(result.samples_ms);
        std::cout << std::left << std::setw(22) << result.name
                  << std::right << std::setw(10) << result.samples_ms.size()
                  << std::setw(14) << result.logical_nodes
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.mean_ms
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.min_ms
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.max_ms
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.p95_ms
                  << '\n';
    }
}

BaselineMap load_baselines(const std::filesystem::path& path)
{
    std::ifstream stream(path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Failed to open baseline file: " + path.string());
    }
    std::string content((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
    BaselineMap baselines;

    std::size_t cursor = 0U;
    const std::string name_token = "\"name\":\"";
    while ((cursor = content.find(name_token, cursor)) != std::string::npos) {
        cursor += name_token.size();
        const auto name_end = content.find('"', cursor);
        if (name_end == std::string::npos) {
            break;
        }
        std::string name = content.substr(cursor, name_end - cursor);

        auto locate_numeric = [&](std::string_view label, std::size_t start) -> std::pair<double, std::size_t> {
            auto value_pos = content.find(label, start);
            if (value_pos == std::string::npos) {
                throw std::runtime_error("Baseline missing field " + std::string(label));
            }
            value_pos += label.size();
            while (value_pos < content.size() && std::isspace(static_cast<unsigned char>(content[value_pos]))) {
                ++value_pos;
            }
            const auto value_end = content.find_first_of(",}", value_pos);
            if (value_end == std::string::npos) {
                throw std::runtime_error("Malformed baseline numeric value for " + std::string(label));
            }
            double value = std::stod(content.substr(value_pos, value_end - value_pos));
            return {value, value_end};
        };

        auto [mean_ms, mean_end] = locate_numeric("\"mean_ms\":", name_end);
        auto [p95_ms, p95_end] = locate_numeric("\"p95_ms\":", mean_end);
        baselines[name] = BaselineEntry{mean_ms, p95_ms};
        cursor = p95_end;
    }

    if (baselines.empty()) {
        throw std::runtime_error("Baseline file contained no benchmark entries");
    }

    return baselines;
}

std::vector<std::string> evaluate_baselines(const std::vector<BenchmarkResult>& results,
                                            const BaselineMap& baselines,
                                            double tolerance)
{
    std::vector<std::string> failures;
    for (const auto& result : results) {
        auto it = baselines.find(result.name);
        if (it == baselines.end()) {
            continue;
        }
        const auto summary = summarise(result.samples_ms);
        const auto& baseline = it->second;
        const double mean_budget = baseline.mean_ms * (1.0 + tolerance);
        const double p95_budget = baseline.p95_ms * (1.0 + tolerance);

        if (summary.mean_ms > mean_budget) {
            std::ostringstream oss;
            oss << result.name << ": mean " << std::fixed << std::setprecision(3) << summary.mean_ms
                << "ms exceeds baseline " << baseline.mean_ms << "ms by more than " << tolerance * 100.0 << '%';
            failures.push_back(oss.str());
        }
        if (summary.p95_ms > p95_budget) {
            std::ostringstream oss;
            oss << result.name << ": p95 " << std::fixed << std::setprecision(3) << summary.p95_ms
                << "ms exceeds baseline " << baseline.p95_ms << "ms by more than " << tolerance * 100.0 << '%';
            failures.push_back(oss.str());
        }
    }
    return failures;
}

}  // namespace

int main(int argc, char** argv)
{
    try {
        const auto options = parse_options(argc, argv);
        std::vector<BenchmarkResult> results;
        results.reserve(3U);
        results.push_back(benchmark_select(options));
        results.push_back(benchmark_join(options));
        results.push_back(benchmark_update(options));

        if (options.json_output) {
            print_json(results);
        } else {
            print_table(results);
        }

        if (options.baseline_path) {
            const auto baselines = load_baselines(*options.baseline_path);
            const auto failures = evaluate_baselines(results, baselines, options.tolerance);
            if (!failures.empty()) {
                std::cerr << "Planner benchmark regressions detected (tolerance "
                          << options.tolerance * 100.0 << "%):\n";
                for (const auto& failure : failures) {
                    std::cerr << "  - " << failure << '\n';
                }
                return 2;
            }
        }

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Planner benchmark harness failed: " << ex.what() << '\n';
        return 1;
    }
}
