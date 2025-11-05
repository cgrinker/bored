#include "bored/executor/executor_context.hpp"
#include "bored/executor/spool_executor.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/worktable_registry.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_retention.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/txn/snapshot_utils.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <regex>
#include <numeric>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <unordered_map>

namespace bs = bored::storage;

namespace {

std::span<const std::byte> tuple_payload_view(std::span<const std::byte> storage)
{
    if (storage.size() <= bs::tuple_header_size()) {
        return {};
    }
    return storage.subspan(bs::tuple_header_size());
}

std::vector<std::byte> tuple_payload_vector(std::span<const std::byte> storage)
{
    auto payload = tuple_payload_view(storage);
    return std::vector<std::byte>(payload.begin(), payload.end());
}

bs::WalCommitHeader make_commit_header(const std::shared_ptr<bs::WalWriter>& wal_writer,
                                       std::uint64_t transaction_id,
                                       std::uint64_t next_transaction_id = 0U,
                                       std::uint64_t oldest_active_txn = 0U,
                                       std::uint64_t oldest_active_commit_lsn = 0U)
{
    bs::WalCommitHeader header{};
    header.transaction_id = transaction_id;
    header.commit_lsn = wal_writer ? wal_writer->next_lsn() : 0U;
    header.next_transaction_id = next_transaction_id != 0U ? next_transaction_id : (transaction_id + 1U);
    header.oldest_active_transaction_id = oldest_active_txn;
    header.oldest_active_commit_lsn = oldest_active_commit_lsn != 0U ? oldest_active_commit_lsn : header.commit_lsn;
    return header;
}

std::vector<std::vector<std::byte>> collect_page_rows(std::span<const std::byte> page,
                                                      const std::vector<std::uint16_t>& slots)
{
    std::vector<std::vector<std::byte>> rows;
    rows.reserve(slots.size());
    for (auto slot : slots) {
        auto tuple = bs::read_tuple(page, slot);
        rows.emplace_back(tuple.begin(), tuple.end());
    }
    return rows;
}

class SpoolBenchmarkChild final : public bored::executor::ExecutorNode {
public:
    SpoolBenchmarkChild(const std::vector<std::vector<std::byte>>* rows, std::size_t* iterations) noexcept
        : rows_{rows}
        , iterations_{iterations}
    {
    }

    void open(bored::executor::ExecutorContext&) override { index_ = 0U; }

    bool next(bored::executor::ExecutorContext&, bored::executor::TupleBuffer& buffer) override
    {
        if (rows_ == nullptr || index_ >= rows_->size()) {
            return false;
        }

        buffer.reset();
        const auto& row = (*rows_)[index_++];
        buffer.write(std::span<const std::byte>(row.data(), row.size()));
        if (iterations_ != nullptr) {
            ++(*iterations_);
        }
        return true;
    }

    void close(bored::executor::ExecutorContext&) override {}

private:
    const std::vector<std::vector<std::byte>>* rows_ = nullptr;
    std::size_t* iterations_ = nullptr;
    std::size_t index_ = 0U;
};

struct BenchmarkOptions final {
    std::size_t samples = 5U;
    std::size_t fsm_page_count = 2048U;
    std::size_t fsm_iterations = 32U;
    std::size_t retention_segment_count = 12U;
    std::size_t retention_keep_segments = 4U;
    std::size_t retention_records_per_segment = 6U;
    std::size_t overflow_page_count = 64U;
    std::size_t overflow_payload_bytes = 16384U;
    std::size_t spool_row_count = 256U;
    std::size_t spool_iterations = 4U;
    bool json_output = false;
    std::optional<std::filesystem::path> baseline_path{};
    double tolerance = 0.15;  // 15% default budget
};

struct BenchmarkResult final {
    std::string name{};
    std::vector<double> samples_ms{};
    std::size_t work_units = 0U;
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

class TempDirectory final {
public:
    explicit TempDirectory(std::filesystem::path path)
        : path_{std::move(path)}
    {
        std::filesystem::create_directories(path_);
    }

    TempDirectory(TempDirectory&& other) noexcept = default;
    TempDirectory& operator=(TempDirectory&& other) noexcept = default;

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    ~TempDirectory()
    {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_{};
};

[[nodiscard]] TempDirectory make_temp_directory(const std::string& prefix)
{
    auto root = std::filesystem::temp_directory_path();
    auto unique = prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    return TempDirectory{root / unique};
}

[[noreturn]] void usage()
{
    std::cerr << "Usage: bored_benchmarks [options]\n"
              << "  --samples=N                 Number of samples per benchmark (default 5)\n"
              << "  --fsm-pages=N               Pages to include in FSM refresh workload (default 2048)\n"
              << "  --fsm-iterations=N          Iterations over the FSM workload (default 32)\n"
              << "  --retention-segments=N      Segments to generate before retention pruning (default 12)\n"
              << "  --retention-keep=N          Segments to retain after pruning (default 4)\n"
              << "  --overflow-pages=N          Overflow tuples to generate for replay (default 64)\n"
              << "  --overflow-bytes=N          Payload bytes per overflow tuple (default 16384)\n"
              << "  --spool-rows=N              Rows to materialise per spool recovery sample (default 256)\n"
              << "  --spool-iterations=N        Spool executions per sample (default 4; include cached reuse)\n"
              << "  --baseline=PATH             Load baseline JSON to enforce regression thresholds\n"
              << "  --tolerance=FRACTION       Allowable fractional regression over baseline (default 0.15)\n"
              << "  --json                      Emit JSON summary instead of table output\n"
              << "  --help                      Show this message\n";
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

void throw_if_error(const std::error_code& ec, std::string_view context)
{
    if (ec) {
        throw std::runtime_error(std::string(context) + ": " + ec.message());
    }
}

std::shared_ptr<bs::AsyncIo> make_async_io()
{
    bs::AsyncIoConfig config{};
    config.backend = bs::AsyncIoBackend::ThreadPool;
    config.worker_threads = 2U;
    config.queue_depth = 64U;
    auto instance = bs::create_async_io(config);
    return std::shared_ptr<bs::AsyncIo>(std::move(instance));
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
    const auto index = static_cast<std::size_t>(std::ceil(0.95 * static_cast<double>(sorted.size()))) - 1U;
    summary.p95_ms = sorted[std::min(index, sorted.size() - 1U)];
    return summary;
}

[[nodiscard]] std::string normalise_baseline_content(std::string raw)
{
    if (raw.size() >= 2U) {
        const auto first = static_cast<unsigned char>(raw[0]);
        const auto second = static_cast<unsigned char>(raw[1]);
        if (first == 0xFFU && second == 0xFEU) {
            // UTF-16 LE
            std::string utf8;
            utf8.reserve(raw.size() / 2U);
            for (std::size_t index = 2U; index + 1U < raw.size(); index += 2U) {
                const auto lo = static_cast<unsigned char>(raw[index]);
                const auto hi = static_cast<unsigned char>(raw[index + 1U]);
                const char16_t code_unit = static_cast<char16_t>(lo | (static_cast<char16_t>(hi) << 8U));
                if (code_unit <= 0x7F) {
                    utf8.push_back(static_cast<char>(code_unit));
                } else {
                    throw std::runtime_error("Baseline file contains non-ASCII UTF-16 characters");
                }
            }
            return utf8;
        }
        if (first == 0xFEU && second == 0xFFU) {
            // UTF-16 BE
            std::string utf8;
            utf8.reserve(raw.size() / 2U);
            for (std::size_t index = 2U; index + 1U < raw.size(); index += 2U) {
                const auto hi = static_cast<unsigned char>(raw[index]);
                const auto lo = static_cast<unsigned char>(raw[index + 1U]);
                const char16_t code_unit = static_cast<char16_t>((static_cast<char16_t>(hi) << 8U) | lo);
                if (code_unit <= 0x7F) {
                    utf8.push_back(static_cast<char>(code_unit));
                } else {
                    throw std::runtime_error("Baseline file contains non-ASCII UTF-16 characters");
                }
            }
            return utf8;
        }
    }

    return raw;
}

[[nodiscard]] BaselineMap load_baseline_file(const std::filesystem::path& path)
{
    std::ifstream stream(path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Failed to open baseline file: " + path.string());
    }

    std::string raw((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
    auto content = normalise_baseline_content(std::move(raw));

    static const std::regex entry_regex(R"(\{\"name\":\"([^\"]+)\"[^}]*\"mean_ms\":([-+0-9eE\.]+)[^}]*\"p95_ms\":([-+0-9eE\.]+))");

    BaselineMap baselines;
    for (auto it = std::sregex_iterator(content.begin(), content.end(), entry_regex);
         it != std::sregex_iterator();
         ++it) {
        const auto& match = *it;
        if (match.size() != 4) {
            continue;
        }

        std::string name = match[1].str();
        double mean_ms = 0.0;
        double p95_ms = 0.0;

        try {
            mean_ms = std::stod(match[2].str());
            p95_ms = std::stod(match[3].str());
        } catch (const std::exception& ex) {
            throw std::runtime_error(std::string{"Failed to parse baseline entry for "} + name + ": " + ex.what());
        }

        baselines[name] = BaselineEntry{mean_ms, p95_ms};
    }

    if (baselines.empty()) {
        throw std::runtime_error("Baseline file contained no benchmark entries");
    }

    return baselines;
}

[[nodiscard]] std::vector<std::string> evaluate_baselines(const std::vector<BenchmarkResult>& results,
                                                          const BaselineMap& baselines,
                                                          double tolerance)
{
    std::vector<std::string> failures;
    for (const auto& result : results) {
        auto baseline_it = baselines.find(result.name);
        if (baseline_it == baselines.end()) {
            continue;
        }

        const auto summary = summarise(result.samples_ms);
        const auto& baseline = baseline_it->second;
        const auto mean_budget = baseline.mean_ms * (1.0 + tolerance);
        const auto p95_budget = baseline.p95_ms * (1.0 + tolerance);

        if (summary.mean_ms > mean_budget) {
            std::ostringstream oss;
            oss << result.name << ": mean " << std::fixed << std::setprecision(3) << summary.mean_ms
                << "ms exceeded baseline " << baseline.mean_ms << "ms by more than " << tolerance * 100.0 << "%";
            failures.push_back(oss.str());
        }
        if (summary.p95_ms > p95_budget) {
            std::ostringstream oss;
            oss << result.name << ": p95 " << std::fixed << std::setprecision(3) << summary.p95_ms
                << "ms exceeded baseline " << baseline.p95_ms << "ms by more than " << tolerance * 100.0 << "%";
            failures.push_back(oss.str());
        }
    }
    return failures;
}

struct OverflowFixture final {
    struct CrashPage final {
        std::uint32_t page_id = 0U;
        std::vector<std::byte> image{};
    };

    struct ExpectedTuple final {
        std::uint32_t page_id = 0U;
        std::uint16_t slot_index = 0U;
        std::vector<std::byte> payload{};
    };

    bs::WalRecoveryPlan plan{};
    std::vector<CrashPage> crash_pages{};
    std::vector<ExpectedTuple> expected{};
};

OverflowFixture build_overflow_fixture(const BenchmarkOptions& options)
{
    OverflowFixture fixture{};

    auto wal_dir_holder = make_temp_directory("bored_bench_overflow_");
    const auto& wal_dir = wal_dir_holder.path();

    auto io = make_async_io();

    bs::WalWriterConfig writer_config{};
    writer_config.directory = wal_dir;
    writer_config.start_lsn = bs::kWalBlockSize;

    const std::size_t payload_bytes = std::max<std::size_t>(options.overflow_payload_bytes, 8192U);
    const auto tuned_buffer_bytes = bs::align_up_to_block(std::max<std::size_t>(8U * bs::kWalBlockSize,
                                                                                payload_bytes * 2U));
    const auto tuned_segment_bytes = bs::align_up_to_block(std::max<std::size_t>(16U * bs::kWalBlockSize,
                                                                                 tuned_buffer_bytes * 2U));

    writer_config.buffer_size = tuned_buffer_bytes;
    writer_config.segment_size = tuned_segment_bytes;

    auto wal_writer = std::make_shared<bs::WalWriter>(io, writer_config);
    bs::FreeSpaceMap fsm;
    bs::PageManager manager{&fsm, wal_writer};

    const std::size_t tuple_count = std::max<std::size_t>(options.overflow_page_count, 1U);

    for (std::size_t index = 0; index < tuple_count; ++index) {
        const std::uint32_t page_id = static_cast<std::uint32_t>(50000U + index);
        alignas(8) std::array<std::byte, bs::kPageSize> page_buffer{};
        auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
        throw_if_error(manager.initialize_page(page_span, bs::PageType::Table, page_id), "initialize_page");

        std::vector<std::byte> initial_payload(payload_bytes);
        for (std::size_t payload_index = 0; payload_index < initial_payload.size(); ++payload_index) {
            initial_payload[payload_index] = static_cast<std::byte>((payload_index + index) & 0xFFU);
        }

        bs::PageManager::TupleInsertResult insert_result{};
        throw_if_error(manager.insert_tuple(page_span,
                                            std::span<const std::byte>(initial_payload.data(), initial_payload.size()),
                                            0xABC000ULL + index,
                                            insert_result),
                       "insert_tuple");
        if (!insert_result.used_overflow) {
            continue;
        }

    bs::WalCommitHeader commit_header{};
    commit_header.transaction_id = page_id;
    commit_header.commit_lsn = wal_writer->next_lsn();
    commit_header.next_transaction_id = page_id + 1U;
    commit_header.oldest_active_transaction_id = page_id;
    commit_header.oldest_active_commit_lsn = commit_header.commit_lsn;
    bs::WalAppendResult commit_result{};
    throw_if_error(wal_writer->append_commit_record(commit_header, commit_result), "append commit record");

        std::array<std::byte, 256> shrink_payload{};
        shrink_payload.fill(std::byte{0x7A});

        bs::PageManager::TupleUpdateResult update_result{};
        throw_if_error(manager.update_tuple(page_span,
                                            insert_result.slot.index,
                                            std::span<const std::byte>(shrink_payload.data(), shrink_payload.size()),
                                            0xABC000ULL + index,
                                            update_result),
                       "update_tuple");

        fixture.crash_pages.push_back(OverflowFixture::CrashPage{page_id, std::vector<std::byte>(page_buffer.begin(), page_buffer.end())});
    }

    throw_if_error(manager.flush_wal(), "flush_wal");
    throw_if_error(manager.close_wal(), "close_wal");
    io->shutdown();

    bs::WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    throw_if_error(driver.build_plan(fixture.plan), "build_plan");

    for (const auto& record : fixture.plan.undo) {
        if (static_cast<bs::WalRecordType>(record.header.type) == bs::WalRecordType::TupleBeforeImage) {
            auto view = bs::decode_wal_tuple_before_image(std::span<const std::byte>(record.payload.data(), record.payload.size()));
            if (!view) {
                continue;
            }
            OverflowFixture::ExpectedTuple expected{};
            expected.page_id = view->meta.page_id;
            expected.slot_index = view->meta.slot_index;
            auto before_payload = tuple_payload_view(view->tuple_payload);
            expected.payload.assign(before_payload.begin(), before_payload.end());
            fixture.expected.push_back(std::move(expected));
        }
    }

    return fixture;
}

BenchmarkResult benchmark_fsm_refresh(const BenchmarkOptions& options)
{
    BenchmarkResult result{};
    result.name = "fsm_refresh";
    result.work_units = options.fsm_page_count * options.fsm_iterations;

    std::vector<std::array<std::byte, bs::kPageSize>> pages(options.fsm_page_count);

    for (std::size_t sample = 0; sample < options.samples; ++sample) {
        bs::FreeSpaceMap fsm;

        for (std::size_t index = 0; index < pages.size(); ++index) {
            auto span = std::span<std::byte>(pages[index].data(), pages[index].size());
            if (!bs::initialize_page(span, bs::PageType::Table, static_cast<std::uint32_t>(1000U + index))) {
                throw std::runtime_error("initialize_page failed");
            }
        }

        const auto start = std::chrono::steady_clock::now();

        for (std::size_t iteration = 0; iteration < options.fsm_iterations; ++iteration) {
            for (std::size_t index = 0; index < pages.size(); ++index) {
                auto span = std::span<std::byte>(pages[index].data(), pages[index].size());
                auto& header = bs::page_header(span);
                const auto adjustment = static_cast<std::uint16_t>((iteration * 13U + index * 7U) % (bs::kPageSize / 2U));
                header.free_start = static_cast<std::uint16_t>(sizeof(bs::PageHeader) + adjustment);
                header.free_end = static_cast<std::uint16_t>(bs::kPageSize - (adjustment % 256U) - 128U);
                if (header.free_end <= header.free_start) {
                    header.free_end = static_cast<std::uint16_t>(header.free_start + 64U);
                }
                bs::sync_free_space(fsm, std::span<const std::byte>(pages[index].data(), pages[index].size()));
            }
        }

        const auto end = std::chrono::steady_clock::now();
        const auto duration = std::chrono::duration<double, std::milli>(end - start).count();
        result.samples_ms.push_back(duration);
    }

    return result;
}

BenchmarkResult benchmark_retention_pruning(const BenchmarkOptions& options)
{
    BenchmarkResult result{};
    result.name = "wal_retention_prune";
    result.work_units = options.retention_segment_count;

    for (std::size_t sample = 0; sample < options.samples; ++sample) {
        auto wal_dir_holder = make_temp_directory("bored_bench_retention_");
        const auto& wal_dir = wal_dir_holder.path();
        const auto archive_dir = wal_dir / "archive";
        std::filesystem::create_directories(archive_dir);

        auto io = make_async_io();

        bs::WalWriterConfig writer_config{};
        writer_config.directory = wal_dir;
        writer_config.segment_size = 4U * bs::kWalBlockSize;
        writer_config.buffer_size = 2U * bs::kWalBlockSize;
        writer_config.start_lsn = bs::kWalBlockSize;
        writer_config.retention.retention_segments = options.retention_keep_segments;
        writer_config.retention.archive_path = archive_dir;

        auto wal_writer = std::make_shared<bs::WalWriter>(io, writer_config);

        std::vector<std::byte> tuple_payload(512U);
        for (std::size_t index = 0; index < tuple_payload.size(); ++index) {
            tuple_payload[index] = static_cast<std::byte>(index & 0xFFU);
        }

        bs::WalTupleMeta tuple_meta{};
        tuple_meta.page_id = 9000U;
        tuple_meta.slot_index = 0U;
        tuple_meta.tuple_length = static_cast<std::uint16_t>(tuple_payload.size());
        tuple_meta.row_id = 0U;

        std::vector<std::byte> wal_buffer(bs::wal_tuple_insert_payload_size(tuple_meta.tuple_length));
        auto wal_buffer_span = std::span<std::byte>(wal_buffer.data(), wal_buffer.size());

        bs::WalRecordDescriptor descriptor{};
        descriptor.type = bs::WalRecordType::TupleInsert;
        descriptor.flags = bs::WalRecordFlag::HasPayload;

        bs::WalAppendResult append_result{};
        std::uint64_t last_segment_id = 0U;

        const std::size_t segments_to_generate = std::max<std::size_t>(options.retention_segment_count, 1U);
        const std::size_t records_per_segment = std::max<std::size_t>(options.retention_records_per_segment, 1U);

        for (std::size_t segment = 0; segment < segments_to_generate; ++segment) {
            for (std::size_t record = 0; record < records_per_segment; ++record) {
                tuple_meta.row_id = static_cast<std::uint64_t>(segment * records_per_segment + record);
                tuple_meta.page_id = static_cast<std::uint32_t>(9000U + record);
                if (!bs::encode_wal_tuple_insert(wal_buffer_span,
                                                 tuple_meta,
                                                 std::span<const std::byte>(tuple_payload.data(), tuple_payload.size()))) {
                    throw std::runtime_error("encode_wal_tuple_insert failed");
                }
                descriptor.page_id = tuple_meta.page_id;
                descriptor.payload = std::span<const std::byte>(wal_buffer.data(), wal_buffer.size());
                throw_if_error(wal_writer->append_record(descriptor, append_result), "append_record");
                last_segment_id = append_result.segment_id;
            }
        }

        throw_if_error(wal_writer->flush(), "wal_writer::flush");
        throw_if_error(wal_writer->close(), "wal_writer::close");
        io->shutdown();

        bs::WalRetentionManager retention{wal_dir, writer_config.file_prefix, writer_config.file_extension};
        bs::WalRetentionConfig retention_config = writer_config.retention;

        const auto start = std::chrono::steady_clock::now();
        bs::WalRetentionStats stats{};
        throw_if_error(retention.apply(retention_config, last_segment_id, &stats), "WalRetentionManager::apply");
        const auto end = std::chrono::steady_clock::now();
        const auto duration = std::chrono::duration<double, std::milli>(end - start).count();
        result.samples_ms.push_back(duration);
    }

    return result;
}

BenchmarkResult benchmark_overflow_replay(const BenchmarkOptions& options)
{
    BenchmarkResult result{};
    result.name = "wal_overflow_replay";
    result.work_units = options.overflow_page_count;

    for (std::size_t sample = 0; sample < options.samples; ++sample) {
        auto fixture = build_overflow_fixture(options);

        bs::FreeSpaceMap replay_fsm;
        bs::WalReplayContext context{bs::PageType::Table, &replay_fsm};

        for (const auto& crash_page : fixture.crash_pages) {
            context.set_page(crash_page.page_id,
                             std::span<const std::byte>(crash_page.image.data(), crash_page.image.size()));
        }

        bs::WalReplayer replayer{context};

        const auto start = std::chrono::steady_clock::now();
        throw_if_error(replayer.apply_redo(fixture.plan), "WalReplayer::apply_redo");
        throw_if_error(replayer.apply_undo(fixture.plan), "WalReplayer::apply_undo");
        const auto end = std::chrono::steady_clock::now();

        for (const auto& expected : fixture.expected) {
            auto page = context.get_page(expected.page_id);
            auto tuple = bs::read_tuple(std::span<const std::byte>(page.data(), page.size()), expected.slot_index);
            if (tuple.size() != expected.payload.size() ||
                !std::equal(tuple.begin(), tuple.end(), expected.payload.begin(), expected.payload.end())) {
                throw std::runtime_error("Overflow replay verification failed");
            }
        }

        const auto duration = std::chrono::duration<double, std::milli>(end - start).count();
        result.samples_ms.push_back(duration);
    }

    return result;
}

BenchmarkResult benchmark_spool_recovery(const BenchmarkOptions& options)
{
    BenchmarkResult result{};
    result.name = "spool_worktable_recovery";
    const std::size_t row_count = std::max<std::size_t>(options.spool_row_count, 1U);
    const std::size_t iterations = std::max<std::size_t>(options.spool_iterations, 1U);
    result.work_units = row_count * iterations;

    for (std::size_t sample = 0; sample < options.samples; ++sample) {
        auto wal_dir_holder = make_temp_directory("bored_bench_spool_recovery_");
        const auto& wal_dir = wal_dir_holder.path();

        auto io = make_async_io();

        bs::WalWriterConfig writer_config{};
        writer_config.directory = wal_dir;
        writer_config.segment_size = 4U * bs::kWalBlockSize;
        writer_config.buffer_size = 2U * bs::kWalBlockSize;
        writer_config.start_lsn = bs::kWalBlockSize;

        auto wal_writer = std::make_shared<bs::WalWriter>(io, writer_config);
        bs::FreeSpaceMap fsm;
        bs::PageManager manager{&fsm, wal_writer};

        const std::uint32_t page_id = static_cast<std::uint32_t>(61000U + sample);
        alignas(8) std::array<std::byte, bs::kPageSize> page_buffer{};
        auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
        throw_if_error(manager.initialize_page(page_span, bs::PageType::Table, page_id), "initialize_page");

        std::vector<std::uint16_t> slot_order;
        slot_order.reserve(row_count);

        for (std::size_t index = 0; index < row_count; ++index) {
            std::vector<std::byte> payload(48U + static_cast<std::size_t>((index % 7U) * 8U));
            for (std::size_t pos = 0; pos < payload.size(); ++pos) {
                payload[pos] = static_cast<std::byte>(((index + 3U) * (pos + 5U)) & 0xFFU);
            }

            bs::PageManager::TupleInsertResult insert_result{};
            throw_if_error(manager.insert_tuple(page_span,
                                                std::span<const std::byte>(payload.data(), payload.size()),
                                                0xBEE000ULL + index,
                                                insert_result),
                           "insert_tuple");
            slot_order.push_back(insert_result.slot.index);
        }

        auto baseline_rows = collect_page_rows(std::span<const std::byte>(page_span.data(), page_span.size()), slot_order);

        auto commit_header = make_commit_header(wal_writer, 9000U + sample, 9001U + sample, 9000U + sample);
        bs::WalAppendResult commit_result{};
        throw_if_error(wal_writer->append_commit_record(commit_header, commit_result), "append_commit_record");
        throw_if_error(wal_writer->flush(), "wal_writer::flush");
        throw_if_error(manager.flush_wal(), "PageManager::flush_wal");
        throw_if_error(manager.close_wal(), "PageManager::close_wal");
        throw_if_error(wal_writer->close(), "wal_writer::close");
        io->shutdown();

        const auto start = std::chrono::steady_clock::now();

        bs::WalRecoveryDriver driver{wal_dir, writer_config.file_prefix, writer_config.file_extension, nullptr, wal_dir / "checkpoints"};
        bs::WalRecoveryPlan plan{};
        throw_if_error(driver.build_plan(plan), "WalRecoveryDriver::build_plan");

        bs::WalReplayContext replay_context{bs::PageType::Table, nullptr};
        bs::WalReplayer replayer{replay_context};
        throw_if_error(replayer.apply_redo(plan), "WalReplayer::apply_redo");
        throw_if_error(replayer.apply_undo(plan), "WalReplayer::apply_undo");

        auto replay_page = replay_context.get_page(page_id);
        auto replay_rows = collect_page_rows(std::span<const std::byte>(replay_page.data(), replay_page.size()), slot_order);
        if (replay_rows != baseline_rows) {
            throw std::runtime_error("Spool recovery benchmark detected mismatched replay rows");
        }

        bored::executor::WorkTableRegistry registry;
        bored::executor::SpoolExecutor::Config spool_config{};
        spool_config.reserve_rows = replay_rows.size();
        spool_config.worktable_registry = &registry;
        spool_config.worktable_id = 0xC0DEC0DEU;

        bored::txn::Snapshot replay_snapshot{};
        const std::uint64_t replay_lsn = commit_result.lsn != 0U ? commit_result.lsn : commit_header.commit_lsn;
        replay_snapshot.read_lsn = replay_lsn;
        replay_snapshot.xmin = commit_header.transaction_id;
        replay_snapshot.xmax = commit_header.next_transaction_id;

        bored::executor::ExecutorContext executor_context{};
        executor_context.set_snapshot(replay_snapshot);

        std::size_t child_reads = 0U;
        bored::executor::TupleBuffer tuple_buffer{};

        for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
            auto child = std::make_unique<SpoolBenchmarkChild>(&replay_rows, &child_reads);
            bored::executor::SpoolExecutor spool{std::move(child), spool_config};
            spool.open(executor_context);
            while (spool.next(executor_context, tuple_buffer)) {
                tuple_buffer.reset();
            }
            spool.close(executor_context);
        }

        if (iterations > 0U && child_reads != row_count) {
            throw std::runtime_error("Spool recovery benchmark expected single materialisation pass");
        }

        const auto end = std::chrono::steady_clock::now();
        result.samples_ms.push_back(std::chrono::duration<double, std::milli>(end - start).count());
    }

    return result;
}

void print_json(const std::vector<BenchmarkResult>& results)
{
    std::cout << "{\"benchmarks\":[";
    for (std::size_t index = 0; index < results.size(); ++index) {
        const auto& result = results[index];
        auto summary = summarise(result.samples_ms);
        if (index > 0) {
            std::cout << ',';
        }
        std::cout << "{\"name\":\"" << result.name << "\""
                  << ",\"samples\":" << result.samples_ms.size()
                  << ",\"work_units\":" << result.work_units
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
    std::cout << std::left << std::setw(28) << "Benchmark"
              << std::right << std::setw(12) << "Samples"
              << std::setw(14) << "Work Units"
              << std::setw(14) << "Mean (ms)"
              << std::setw(14) << "Min (ms)"
              << std::setw(14) << "Max (ms)"
              << std::setw(14) << "P95 (ms)" << '\n';

    for (const auto& result : results) {
        auto summary = summarise(result.samples_ms);
        std::cout << std::left << std::setw(28) << result.name
                  << std::right << std::setw(12) << result.samples_ms.size()
                  << std::setw(14) << result.work_units
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.mean_ms
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.min_ms
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.max_ms
                  << std::setw(14) << std::fixed << std::setprecision(3) << summary.p95_ms
                  << '\n';
    }
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
        } else if (argument.rfind("--fsm-pages=", 0) == 0) {
            options.fsm_page_count = parse_size(argument.substr(12), "--fsm-pages");
        } else if (argument.rfind("--fsm-iterations=", 0) == 0) {
            options.fsm_iterations = parse_size(argument.substr(17), "--fsm-iterations");
        } else if (argument.rfind("--retention-segments=", 0) == 0) {
            options.retention_segment_count = parse_size(argument.substr(21), "--retention-segments");
        } else if (argument.rfind("--retention-keep=", 0) == 0) {
            options.retention_keep_segments = parse_size(argument.substr(17), "--retention-keep");
        } else if (argument.rfind("--overflow-pages=", 0) == 0) {
            options.overflow_page_count = parse_size(argument.substr(17), "--overflow-pages");
        } else if (argument.rfind("--overflow-bytes=", 0) == 0) {
            options.overflow_payload_bytes = parse_size(argument.substr(17), "--overflow-bytes");
        } else if (argument.rfind("--spool-rows=", 0) == 0) {
            options.spool_row_count = parse_size(argument.substr(13), "--spool-rows");
        } else if (argument.rfind("--spool-iterations=", 0) == 0) {
            options.spool_iterations = parse_size(argument.substr(18), "--spool-iterations");
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

}  // namespace

int main(int argc, char** argv)
{
    try {
        const auto options = parse_options(argc, argv);

    std::vector<BenchmarkResult> results;
    results.reserve(4U);
        results.push_back(benchmark_fsm_refresh(options));
        results.push_back(benchmark_retention_pruning(options));
        results.push_back(benchmark_overflow_replay(options));
    results.push_back(benchmark_spool_recovery(options));

        if (options.json_output) {
            print_json(results);
        } else {
            print_table(results);
        }

        if (options.baseline_path) {
            const auto baselines = load_baseline_file(*options.baseline_path);
            const auto failures = evaluate_baselines(results, baselines, options.tolerance);
            if (!failures.empty()) {
                std::cerr << "Benchmark regressions detected (tolerance " << options.tolerance * 100.0 << "%):\n";
                for (const auto& failure : failures) {
                    std::cerr << "  - " << failure << '\n';
                }
                return 2;
            }
        }

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Benchmark harness failed: " << ex.what() << '\n';
        return 1;
    }
}
