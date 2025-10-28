#include "bored/catalog/catalog_ids.hpp"
#include "bored/executor/aggregation_executor.hpp"
#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/filter_executor.hpp"
#include "bored/executor/hash_join_executor.hpp"
#include "bored/executor/projection_executor.hpp"
#include "bored/executor/seq_scan_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"
#include "bored/txn/transaction_types.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using RelationId = bored::catalog::RelationId;
using Snapshot = bored::txn::Snapshot;
using TransactionId = bored::txn::TransactionId;
using TupleBuffer = bored::executor::TupleBuffer;
using TupleColumnView = bored::executor::TupleColumnView;
using TupleView = bored::executor::TupleView;
using TupleWriter = bored::executor::TupleWriter;
using ExecutorTelemetry = bored::executor::ExecutorTelemetry;
using ExecutorTelemetrySnapshot = bored::executor::ExecutorTelemetrySnapshot;
using ExecutorContext = bored::executor::ExecutorContext;
using ExecutorContextConfig = bored::executor::ExecutorContextConfig;

struct FrozenTableTuple final {
    bored::storage::TupleHeader header{};
    std::vector<std::byte> payload{};
};

class FrozenTableCursor final : public bored::storage::TableScanCursor {
public:
    explicit FrozenTableCursor(const std::vector<FrozenTableTuple>* tuples)
        : tuples_{tuples}
    {}

    bool next(bored::storage::TableTuple& out_tuple) override
    {
        if (tuples_ == nullptr || index_ >= tuples_->size()) {
            return false;
        }
        const auto& entry = (*tuples_)[index_++];
        out_tuple.header = entry.header;
        out_tuple.payload = std::span<const std::byte>(entry.payload.data(), entry.payload.size());
        return true;
    }

    void reset() override { index_ = 0U; }

private:
    const std::vector<FrozenTableTuple>* tuples_ = nullptr;
    std::size_t index_ = 0U;
};

class FrozenStorageReader final : public bored::storage::StorageReader {
public:
    void set_table(RelationId relation, std::vector<FrozenTableTuple> tuples)
    {
        tables_[relation.value] = std::move(tuples);
    }

    [[nodiscard]] std::unique_ptr<bored::storage::TableScanCursor> create_table_scan(
        const bored::storage::TableScanConfig& config) override
    {
        auto it = tables_.find(config.relation_id.value);
        if (it == tables_.end()) {
            return std::make_unique<FrozenTableCursor>(nullptr);
        }
        return std::make_unique<FrozenTableCursor>(&it->second);
    }

private:
    std::unordered_map<std::uint64_t, std::vector<FrozenTableTuple>> tables_{};
};

struct alignas(8) PayloadLayout final {
    std::uint64_t key = 0U;
    std::uint64_t value = 0U;
};

FrozenTableTuple make_tuple(std::uint64_t key, std::uint64_t value, std::size_t payload_bytes)
{
    FrozenTableTuple tuple{};
    tuple.header.created_transaction_id = static_cast<TransactionId>(1U);
    tuple.header.deleted_transaction_id = static_cast<TransactionId>(0U);
    tuple.payload.resize(payload_bytes);

    PayloadLayout payload{key, value};
    std::memcpy(tuple.payload.data(), &payload, sizeof(PayloadLayout));
    for (std::size_t offset = sizeof(PayloadLayout); offset < payload_bytes; ++offset) {
        tuple.payload[offset] = static_cast<std::byte>((key + offset) & 0xFFU);
    }

    return tuple;
}

PayloadLayout payload_from_column(const TupleColumnView& column)
{
    PayloadLayout payload{};
    if (column.data.size() < sizeof(PayloadLayout)) {
        return payload;
    }
    std::memcpy(&payload, column.data.data(), sizeof(PayloadLayout));
    return payload;
}

ExecutorContext make_context()
{
    ExecutorContextConfig config{};
    config.transaction_id = static_cast<TransactionId>(42U);
    Snapshot snapshot{};
    snapshot.xmin = static_cast<TransactionId>(1U);
    snapshot.xmax = std::numeric_limits<TransactionId>::max();
    config.snapshot = snapshot;
    return ExecutorContext{config};
}

struct BenchmarkOptions final {
    std::string scenario = "all";
    std::size_t samples = 5U;
    std::size_t warmups = 1U;
    std::size_t rows = 50000U;
    std::size_t payload_bytes = 64U;
    double filter_selectivity = 0.5;
    std::size_t join_build_rows = 40000U;
    std::size_t join_probe_rows = 60000U;
    double join_match_rate = 0.5;
    std::size_t aggregation_groups = 2048U;
    bool json_output = false;
};

[[noreturn]] void usage()
{
    std::cerr << "Usage: bored_executor_benchmarks [options]\n"
              << "  --scenario=name           Scenario to run (all, scan, scan_filter, scan_filter_project, hash_join, aggregation)\n"
              << "  --samples=N              Measured iterations per scenario (default 5)\n"
              << "  --warmups=N              Warm-up iterations before measuring (default 1)\n"
              << "  --rows=N                 Rows supplied to single-input pipelines (default 50000)\n"
              << "  --payload-bytes=N        Bytes per tuple payload (default 64)\n"
              << "  --filter-selectivity=X   Fraction [0,1] passing filter predicate (default 0.5)\n"
              << "  --join-build=N           Build-side rows for hash join (default 40000)\n"
              << "  --join-probe=N           Probe-side rows for hash join (default 60000)\n"
              << "  --join-match-rate=X      Fraction [0,1] of probe rows matching build side (default 0.5)\n"
              << "  --aggregation-groups=N   Distinct groups for aggregation (default 2048)\n"
              << "  --json                   Emit JSON instead of table output\n"
              << "  --help                   Show this message\n";
    std::exit(1);
}

std::size_t parse_size(std::string_view value, std::string_view option)
{
    std::size_t result = 0U;
    const auto* begin = value.data();
    const auto* end = value.data() + value.size();
    if (auto [ptr, ec] = std::from_chars(begin, end, result); ec != std::errc{} || ptr != end) {
        throw std::invalid_argument(std::string{"Invalid value for "} + std::string(option));
    }
    return result;
}

double parse_double(std::string_view value, std::string_view option)
{
    std::string buffer(value);
    std::size_t processed = 0U;
    double result = 0.0;
    try {
        result = std::stod(buffer, &processed);
    } catch (const std::exception&) {
        throw std::invalid_argument(std::string{"Invalid value for "} + std::string(option));
    }

    if (processed != buffer.size()) {
        throw std::invalid_argument(std::string{"Invalid value for "} + std::string(option));
    }

    return result;
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
        } else if (argument.rfind("--scenario=", 0) == 0) {
            options.scenario = std::string(argument.substr(11));
        } else if (argument.rfind("--samples=", 0) == 0) {
            options.samples = parse_size(argument.substr(10), "--samples");
        } else if (argument.rfind("--warmups=", 0) == 0) {
            options.warmups = parse_size(argument.substr(10), "--warmups");
        } else if (argument.rfind("--rows=", 0) == 0) {
            options.rows = parse_size(argument.substr(7), "--rows");
        } else if (argument.rfind("--payload-bytes=", 0) == 0) {
            options.payload_bytes = parse_size(argument.substr(16), "--payload-bytes");
        } else if (argument.rfind("--filter-selectivity=", 0) == 0) {
            options.filter_selectivity = parse_double(argument.substr(21), "--filter-selectivity");
        } else if (argument.rfind("--join-build=", 0) == 0) {
            options.join_build_rows = parse_size(argument.substr(13), "--join-build");
        } else if (argument.rfind("--join-probe=", 0) == 0) {
            options.join_probe_rows = parse_size(argument.substr(13), "--join-probe");
        } else if (argument.rfind("--join-match-rate=", 0) == 0) {
            options.join_match_rate = parse_double(argument.substr(18), "--join-match-rate");
        } else if (argument.rfind("--aggregation-groups=", 0) == 0) {
            options.aggregation_groups = parse_size(argument.substr(21), "--aggregation-groups");
        } else {
            usage();
        }
    }

    options.samples = std::max<std::size_t>(options.samples, 1U);
    options.rows = std::max<std::size_t>(options.rows, 1U);
    options.payload_bytes = std::max<std::size_t>(options.payload_bytes, sizeof(PayloadLayout));
    options.join_build_rows = std::max<std::size_t>(options.join_build_rows, 1U);
    options.join_probe_rows = std::max<std::size_t>(options.join_probe_rows, 1U);
    options.aggregation_groups = std::max<std::size_t>(options.aggregation_groups, 1U);
    options.filter_selectivity = std::clamp(options.filter_selectivity, 0.0, 1.0);
    options.join_match_rate = std::clamp(options.join_match_rate, 0.0, 1.0);

    return options;
}

struct ExecutorPipeline final {
    FrozenStorageReader reader;
    ExecutorTelemetry telemetry;
    std::unique_ptr<bored::executor::ExecutorNode> root;
};

using PipelinePtr = std::unique_ptr<ExecutorPipeline>;

std::vector<FrozenTableTuple> make_table(std::size_t row_count,
                                         std::size_t payload_bytes,
                                         const std::function<PayloadLayout(std::size_t)>& generator)
{
    std::vector<FrozenTableTuple> tuples;
    tuples.reserve(row_count);
    for (std::size_t index = 0; index < row_count; ++index) {
        auto layout = generator(index);
        tuples.push_back(make_tuple(layout.key, layout.value, payload_bytes));
    }
    return tuples;
}

PipelinePtr make_scan_pipeline(const BenchmarkOptions& options)
{
    auto pipeline = std::make_unique<ExecutorPipeline>();

    const RelationId relation{1001U};
    pipeline->reader.set_table(relation, make_table(options.rows, options.payload_bytes, [](std::size_t index) {
        return PayloadLayout{static_cast<std::uint64_t>(index), static_cast<std::uint64_t>(index * 3U + 7U)};
    }));

    bored::executor::SequentialScanExecutor::Config config{};
    config.reader = &pipeline->reader;
    config.relation_id = relation;
    config.telemetry = &pipeline->telemetry;

    pipeline->root = std::make_unique<bored::executor::SequentialScanExecutor>(config);
    return pipeline;
}

PipelinePtr make_scan_filter_pipeline(const BenchmarkOptions& options)
{
    auto pipeline = std::make_unique<ExecutorPipeline>();

    const RelationId relation{2001U};
    pipeline->reader.set_table(relation, make_table(options.rows, options.payload_bytes, [](std::size_t index) {
        return PayloadLayout{static_cast<std::uint64_t>(index), static_cast<std::uint64_t>(index)};
    }));

    bored::executor::SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &pipeline->reader;
    scan_config.relation_id = relation;
    scan_config.telemetry = &pipeline->telemetry;

    auto scan = std::make_unique<bored::executor::SequentialScanExecutor>(scan_config);

    bored::executor::FilterExecutor::Config filter_config{};
    filter_config.telemetry = &pipeline->telemetry;
    const std::size_t mod = 100U;
    const std::size_t threshold = static_cast<std::size_t>(options.filter_selectivity * static_cast<double>(mod));
    filter_config.predicate = [mod, threshold](const TupleView& view, ExecutorContext&) {
        const auto payload = payload_from_column(view.column(1U));
        return (payload.value % mod) < threshold;
    };

    pipeline->root = std::make_unique<bored::executor::FilterExecutor>(std::move(scan), std::move(filter_config));
    return pipeline;
}

PipelinePtr make_scan_filter_projection_pipeline(const BenchmarkOptions& options)
{
    auto pipeline = std::make_unique<ExecutorPipeline>();

    const RelationId relation{3001U};
    pipeline->reader.set_table(relation, make_table(options.rows, options.payload_bytes, [](std::size_t index) {
        return PayloadLayout{static_cast<std::uint64_t>(index), static_cast<std::uint64_t>(index * 5U + 11U)};
    }));

    bored::executor::SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &pipeline->reader;
    scan_config.relation_id = relation;
    scan_config.telemetry = &pipeline->telemetry;
    auto scan = std::make_unique<bored::executor::SequentialScanExecutor>(scan_config);

    bored::executor::FilterExecutor::Config filter_config{};
    filter_config.telemetry = &pipeline->telemetry;
    const std::size_t mod = 100U;
    const std::size_t threshold = static_cast<std::size_t>(options.filter_selectivity * static_cast<double>(mod));
    filter_config.predicate = [mod, threshold](const TupleView& view, ExecutorContext&) {
        const auto payload = payload_from_column(view.column(1U));
        return (payload.value % mod) < threshold;
    };
    auto filter = std::make_unique<bored::executor::FilterExecutor>(std::move(scan), std::move(filter_config));

    bored::executor::ProjectionExecutor::Config proj_config{};
    proj_config.telemetry = &pipeline->telemetry;
    proj_config.projections.push_back([](const TupleView& view, TupleWriter& writer, ExecutorContext&) {
        const auto payload = view.column(1U);
        writer.append_column(payload.data, payload.is_null);
    });
    proj_config.projections.push_back([](const TupleView& view, TupleWriter& writer, ExecutorContext&) {
        const auto payload = payload_from_column(view.column(1U));
        std::array<std::byte, sizeof(std::uint64_t)> doubled{};
        const std::uint64_t value = payload.value * 2U;
        std::memcpy(doubled.data(), &value, sizeof(value));
        writer.append_column(std::span<const std::byte>(doubled.data(), doubled.size()), false);
    });

    pipeline->root = std::make_unique<bored::executor::ProjectionExecutor>(std::move(filter), std::move(proj_config));
    return pipeline;
}

PipelinePtr make_hash_join_pipeline(const BenchmarkOptions& options)
{
    auto pipeline = std::make_unique<ExecutorPipeline>();

    const RelationId build_relation{4001U};
    const RelationId probe_relation{4002U};

    pipeline->reader.set_table(build_relation, make_table(options.join_build_rows, options.payload_bytes, [](std::size_t index) {
        return PayloadLayout{static_cast<std::uint64_t>(index), static_cast<std::uint64_t>(index * 13U + 3U)};
    }));

    const std::size_t matching = static_cast<std::size_t>(options.join_match_rate * static_cast<double>(options.join_probe_rows));
    pipeline->reader.set_table(probe_relation,
                               make_table(options.join_probe_rows, options.payload_bytes, [&](std::size_t index) {
                                   if (index < matching) {
                                       const auto key = static_cast<std::uint64_t>(index % options.join_build_rows);
                                       return PayloadLayout{key, static_cast<std::uint64_t>(key * 17U + 5U)};
                                   }
                                   const auto key = static_cast<std::uint64_t>(options.join_build_rows + index);
                                   return PayloadLayout{key, static_cast<std::uint64_t>(key * 17U + 5U)};
                               }));

    bored::executor::SequentialScanExecutor::Config build_config{};
    build_config.reader = &pipeline->reader;
    build_config.relation_id = build_relation;
    build_config.telemetry = &pipeline->telemetry;
    auto build = std::make_unique<bored::executor::SequentialScanExecutor>(build_config);

    bored::executor::SequentialScanExecutor::Config probe_config{};
    probe_config.reader = &pipeline->reader;
    probe_config.relation_id = probe_relation;
    probe_config.telemetry = &pipeline->telemetry;
    auto probe = std::make_unique<bored::executor::SequentialScanExecutor>(probe_config);

    bored::executor::HashJoinExecutor::Config join_config{};
    join_config.telemetry = &pipeline->telemetry;
    join_config.build_key = [](const TupleView& view, ExecutorContext&) {
        return std::to_string(payload_from_column(view.column(1U)).key);
    };
    join_config.probe_key = [](const TupleView& view, ExecutorContext&) {
        return std::to_string(payload_from_column(view.column(1U)).key);
    };
    join_config.projections.push_back([](const TupleView& build_view,
                                         const TupleView& probe_view,
                                         TupleWriter& writer,
                                         ExecutorContext&) {
        const auto build_payload = payload_from_column(build_view.column(1U));
        const auto probe_payload = payload_from_column(probe_view.column(1U));
        std::array<std::byte, sizeof(std::uint64_t)> build_key_bytes{};
        std::array<std::byte, sizeof(std::uint64_t)> probe_key_bytes{};
        std::memcpy(build_key_bytes.data(), &build_payload.key, sizeof(build_payload.key));
        std::memcpy(probe_key_bytes.data(), &probe_payload.key, sizeof(probe_payload.key));
        writer.append_column(std::span<const std::byte>(build_key_bytes.data(), build_key_bytes.size()), false);
        writer.append_column(std::span<const std::byte>(probe_key_bytes.data(), probe_key_bytes.size()), false);
    });

    pipeline->root = std::make_unique<bored::executor::HashJoinExecutor>(std::move(build), std::move(probe), std::move(join_config));
    return pipeline;
}

PipelinePtr make_aggregation_pipeline(const BenchmarkOptions& options)
{
    auto pipeline = std::make_unique<ExecutorPipeline>();

    const RelationId relation{5001U};
    const std::size_t groups = std::max<std::size_t>(options.aggregation_groups, 1U);
    pipeline->reader.set_table(relation, make_table(options.rows, options.payload_bytes, [groups](std::size_t index) {
        const auto group = static_cast<std::uint64_t>(index % groups);
        return PayloadLayout{group, static_cast<std::uint64_t>(index)};
    }));

    bored::executor::SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &pipeline->reader;
    scan_config.relation_id = relation;
    scan_config.telemetry = &pipeline->telemetry;
    auto scan = std::make_unique<bored::executor::SequentialScanExecutor>(scan_config);

    bored::executor::AggregationExecutor::AggregateDefinition count_def{};
    count_def.state_size = sizeof(std::uint64_t);
    count_def.state_alignment = alignof(std::uint64_t);
    count_def.initialize = [](std::span<std::byte> state) {
        auto* value = reinterpret_cast<std::uint64_t*>(state.data());
        *value = 0U;
    };
    count_def.accumulate = [](std::span<std::byte> state, const TupleView&, ExecutorContext&) {
        auto* value = reinterpret_cast<std::uint64_t*>(state.data());
        *value += 1U;
    };
    count_def.project = [](std::span<const std::byte> state, TupleWriter& writer, ExecutorContext&) {
        writer.append_column(state, false);
    };

    bored::executor::AggregationExecutor::AggregateDefinition sum_def{};
    sum_def.state_size = sizeof(std::uint64_t);
    sum_def.state_alignment = alignof(std::uint64_t);
    sum_def.initialize = [](std::span<std::byte> state) {
        auto* value = reinterpret_cast<std::uint64_t*>(state.data());
        *value = 0U;
    };
    sum_def.accumulate = [](std::span<std::byte> state, const TupleView& row, ExecutorContext&) {
        auto* value = reinterpret_cast<std::uint64_t*>(state.data());
        const auto payload = payload_from_column(row.column(1U));
        *value += payload.value;
    };
    sum_def.project = [](std::span<const std::byte> state, TupleWriter& writer, ExecutorContext&) {
        writer.append_column(state, false);
    };

    bored::executor::AggregationExecutor::Config agg_config{};
    agg_config.telemetry = &pipeline->telemetry;
    agg_config.group_key = [](const TupleView& row, ExecutorContext&) {
        return std::to_string(payload_from_column(row.column(1U)).key);
    };
    agg_config.group_projection = [](const TupleView& row, TupleWriter& writer, ExecutorContext&) {
        const auto payload = payload_from_column(row.column(1U));
        std::array<std::byte, sizeof(std::uint64_t)> key_bytes{};
        std::memcpy(key_bytes.data(), &payload.key, sizeof(payload.key));
        writer.append_column(std::span<const std::byte>(key_bytes.data(), key_bytes.size()), false);
    };
    agg_config.aggregates.push_back(count_def);
    agg_config.aggregates.push_back(sum_def);

    pipeline->root = std::make_unique<bored::executor::AggregationExecutor>(std::move(scan), std::move(agg_config));
    return pipeline;
}

enum class ScenarioKind {
    Scan,
    ScanFilter,
    ScanFilterProjection,
    HashJoin,
    Aggregation,
};

struct ScenarioDefinition final {
    std::string name;
    ScenarioKind kind;
    std::function<PipelinePtr(const BenchmarkOptions&)> builder;
};

const std::vector<ScenarioDefinition> kScenarios = {
    {"scan", ScenarioKind::Scan, &make_scan_pipeline},
    {"scan_filter", ScenarioKind::ScanFilter, &make_scan_filter_pipeline},
    {"scan_filter_project", ScenarioKind::ScanFilterProjection, &make_scan_filter_projection_pipeline},
    {"hash_join", ScenarioKind::HashJoin, &make_hash_join_pipeline},
    {"aggregation", ScenarioKind::Aggregation, &make_aggregation_pipeline},
};

struct SampleResult final {
    double elapsed_ms = 0.0;
    std::uint64_t rows = 0U;
};

struct BenchmarkResult final {
    std::string name;
    std::vector<double> samples_ms;
    std::uint64_t rows_per_iteration = 0U;
};

SampleResult run_iteration(ExecutorPipeline& pipeline)
{
    pipeline.telemetry.reset();
    TupleBuffer buffer{};
    buffer.reset();
    auto context = make_context();

    const auto start = Clock::now();
    pipeline.root->open(context);

    std::uint64_t produced = 0U;
    while (pipeline.root->next(context, buffer)) {
        ++produced;
    }

    pipeline.root->close(context);
    const auto end = Clock::now();

    SampleResult sample{};
    sample.rows = produced;
    sample.elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
    return sample;
}

BenchmarkResult run_benchmark(const ScenarioDefinition& scenario, const BenchmarkOptions& options)
{
    auto pipeline = scenario.builder(options);

    const std::size_t total_iterations = options.samples + options.warmups;
    BenchmarkResult result{};
    result.name = scenario.name;
    result.samples_ms.reserve(options.samples);

    for (std::size_t iteration = 0; iteration < total_iterations; ++iteration) {
        auto sample = run_iteration(*pipeline);
        if (iteration < options.warmups) {
            continue;
        }
        if (result.rows_per_iteration == 0U) {
            result.rows_per_iteration = sample.rows;
        } else if (result.rows_per_iteration != sample.rows) {
            throw std::runtime_error("Row count varied between iterations for scenario " + scenario.name);
        }
        result.samples_ms.push_back(sample.elapsed_ms);
    }

    return result;
}

struct Summary final {
    double mean_ms = 0.0;
    double min_ms = 0.0;
    double max_ms = 0.0;
    double p95_ms = 0.0;
};

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
    const auto index = static_cast<std::size_t>(std::ceil(0.95 * static_cast<double>(sorted.size()))) - 1U;
    summary.p95_ms = sorted[std::min(index, sorted.size() - 1U)];
    return summary;
}

void print_table(const std::vector<BenchmarkResult>& results)
{
    std::cout << std::left << std::setw(24) << "Scenario"
              << std::right << std::setw(10) << "Samples"
              << std::setw(14) << "Rows"
              << std::setw(14) << "Mean (ms)"
              << std::setw(14) << "Rows/s"
              << std::setw(14) << "Min (ms)"
              << std::setw(14) << "Max (ms)"
              << std::setw(14) << "P95 (ms)" << '\n';

    for (const auto& result : results) {
        auto summary = summarise(result.samples_ms);
        const double rows_per_second = (summary.mean_ms > 0.0)
                                           ? static_cast<double>(result.rows_per_iteration) / (summary.mean_ms / 1000.0)
                                           : 0.0;
        std::cout << std::left << std::setw(24) << result.name
                  << std::right << std::setw(10) << result.samples_ms.size()
                  << std::setw(14) << result.rows_per_iteration
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.mean_ms
                  << std::setw(14) << std::fixed << std::setprecision(0) << rows_per_second
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.min_ms
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.max_ms
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.p95_ms
                  << '\n';
        std::cout << std::defaultfloat;
    }
}

void print_json(const std::vector<BenchmarkResult>& results)
{
    std::cout << "{\"benchmarks\":[";
    for (std::size_t index = 0; index < results.size(); ++index) {
        const auto& result = results[index];
        auto summary = summarise(result.samples_ms);
        const double rows_per_second = (summary.mean_ms > 0.0)
                                           ? static_cast<double>(result.rows_per_iteration) / (summary.mean_ms / 1000.0)
                                           : 0.0;
        if (index > 0) {
            std::cout << ',';
        }
        std::cout << "{\"name\":\"" << result.name << "\""
                  << ",\"samples\":" << result.samples_ms.size()
                  << ",\"rows\":" << result.rows_per_iteration
                  << ",\"mean_ms\":" << std::fixed << std::setprecision(3) << summary.mean_ms
                  << ",\"min_ms\":" << std::fixed << std::setprecision(3) << summary.min_ms
                  << ",\"max_ms\":" << std::fixed << std::setprecision(3) << summary.max_ms
                  << ",\"p95_ms\":" << std::fixed << std::setprecision(3) << summary.p95_ms
                  << ",\"rows_per_second\":" << std::fixed << std::setprecision(0) << rows_per_second
                  << '}';
        std::cout << std::defaultfloat;
    }
    std::cout << "]}" << std::endl;
}

const ScenarioDefinition* find_scenario(std::string_view name)
{
    for (const auto& scenario : kScenarios) {
        if (scenario.name == name) {
            return &scenario;
        }
    }
    return nullptr;
}

std::vector<const ScenarioDefinition*> select_scenarios(const BenchmarkOptions& options)
{
    std::vector<const ScenarioDefinition*> selected;
    if (options.scenario == "all") {
        for (const auto& scenario : kScenarios) {
            selected.push_back(&scenario);
        }
        return selected;
    }

    if (auto* scenario = find_scenario(options.scenario)) {
        selected.push_back(scenario);
        return selected;
    }

    throw std::runtime_error("Unknown scenario: " + options.scenario);
}

}  // namespace

int main(int argc, char** argv)
{
    try {
        const auto options = parse_options(argc, argv);
        const auto scenarios = select_scenarios(options);

        std::vector<BenchmarkResult> results;
        results.reserve(scenarios.size());

        for (const auto* scenario : scenarios) {
            results.push_back(run_benchmark(*scenario, options));
        }

        if (options.json_output) {
            print_json(results);
        } else {
            print_table(results);
        }

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Executor benchmark failed: " << ex.what() << '\n';
        return 1;
    }
}
