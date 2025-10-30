#include "bored/catalog/catalog_introspection.hpp"
#include "bored/storage/lock_introspection.hpp"
#include "bored/storage/storage_diagnostics.hpp"
#include "bored/storage/storage_metrics.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"

#include <CLI/CLI.hpp>
#include <replxx.hxx>

#include <chrono>
#include <cctype>
#include <ctime>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

using bored::storage::StorageDiagnosticsDocument;
using bored::storage::StorageTelemetryRegistry;

namespace {

std::string format_timestamp(const std::chrono::system_clock::time_point& tp)
{
    const auto time_value = std::chrono::system_clock::to_time_t(tp);
    std::tm buffer{};
#if defined(_WIN32)
    localtime_s(&buffer, &time_value);
#else
    localtime_r(&time_value, &buffer);
#endif
    std::ostringstream stream;
    stream << std::put_time(&buffer, "%Y-%m-%d %H:%M:%S");
    return stream.str();
}

std::string format_duration_ns(std::uint64_t ns)
{
    if (ns == 0U) {
        return "0 ms";
    }
    constexpr double kNsPerMs = 1'000'000.0;
    const double ms = static_cast<double>(ns) / kNsPerMs;
    std::ostringstream stream;
    const int precision = ms < 1.0 ? 3 : (ms < 10.0 ? 2 : 1);
    stream << std::fixed << std::setprecision(precision) << ms << " ms";
    return stream.str();
}

std::string format_bytes(std::uint64_t bytes)
{
    if (bytes == 0U) {
        return "0 B";
    }
    constexpr double kScale = 1024.0;
    const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    constexpr std::size_t kUnitCount = sizeof(units) / sizeof(units[0]);
    double value = static_cast<double>(bytes);
    std::size_t index = 0U;
    while (value >= kScale && index < kUnitCount - 1U) {
        value /= kScale;
        ++index;
    }
    std::ostringstream stream;
    const int precision = value < 10.0 ? 2 : 1;
    stream << std::fixed << std::setprecision(index == 0U ? 0 : precision) << value << ' ' << units[index];
    return stream.str();
}

std::string trim(std::string_view text)
{
    std::size_t start = 0U;
    while (start < text.size() && std::isspace(static_cast<unsigned char>(text[start])) != 0) {
        ++start;
    }
    std::size_t end = text.size();
    while (end > start && std::isspace(static_cast<unsigned char>(text[end - 1U])) != 0) {
        --end;
    }
    return std::string{text.substr(start, end - start)};
}

std::vector<std::string> split_tokens(std::string_view text)
{
    std::vector<std::string> tokens;
    std::size_t index = 0U;
    while (index < text.size()) {
        while (index < text.size() && std::isspace(static_cast<unsigned char>(text[index])) != 0) {
            ++index;
        }
        if (index >= text.size()) {
            break;
        }
        const std::size_t begin = index;
        while (index < text.size() && std::isspace(static_cast<unsigned char>(text[index])) == 0) {
            ++index;
        }
        tokens.emplace_back(text.substr(begin, index - begin));
    }
    return tokens;
}

bored::storage::CheckpointTelemetrySnapshot make_mock_checkpoint()
{
    bored::storage::CheckpointTelemetrySnapshot snapshot{};
    snapshot.emitted_checkpoints = 12U;
    snapshot.last_checkpoint_lsn = 93'120U;
    snapshot.last_checkpoint_timestamp_ns = 1'696'000'000'000'000ULL;
    snapshot.last_checkpoint_duration_ns = 24'000'000U;
    snapshot.max_checkpoint_duration_ns = 52'000'000U;
    snapshot.last_queue_depth = 4U;
    snapshot.max_queue_depth = 9U;
    snapshot.blocked_transactions = 2U;
    snapshot.queue_waits = 6U;
    snapshot.last_emit_duration_ns = 9'000'000U;
    snapshot.last_fence_duration_ns = 3'500'000U;
    snapshot.max_fence_duration_ns = 6'750'000U;
    return snapshot;
}

bored::storage::RecoveryTelemetrySnapshot make_mock_recovery()
{
    bored::storage::RecoveryTelemetrySnapshot snapshot{};
    snapshot.plan_invocations = 8U;
    snapshot.plan_failures = 1U;
    snapshot.total_plan_duration_ns = 82'000'000U;
    snapshot.last_plan_duration_ns = 18'000'000U;
    snapshot.max_plan_duration_ns = 24'500'000U;
    snapshot.redo_invocations = 6U;
    snapshot.redo_failures = 0U;
    snapshot.total_redo_duration_ns = 46'000'000U;
    snapshot.last_redo_duration_ns = 11'000'000U;
    snapshot.max_redo_duration_ns = 15'000'000U;
    snapshot.last_replay_backlog_bytes = 25'165'824U;  // ~24 MiB
    snapshot.max_replay_backlog_bytes = 41'943'040U;   // ~40 MiB
    snapshot.undo_invocations = 3U;
    snapshot.undo_failures = 0U;
    snapshot.total_undo_duration_ns = 12'500'000U;
    snapshot.last_undo_duration_ns = 4'000'000U;
    snapshot.cleanup_invocations = 3U;
    snapshot.total_cleanup_duration_ns = 6'000'000U;
    snapshot.last_cleanup_duration_ns = 2'500'000U;
    return snapshot;
}

bored::txn::TransactionTelemetrySnapshot make_mock_transactions()
{
    bored::txn::TransactionTelemetrySnapshot snapshot{};
    snapshot.active_transactions = 5U;
    snapshot.committed_transactions = 1'248U;
    snapshot.aborted_transactions = 21U;
    snapshot.last_snapshot_xmin = 9'204U;
    snapshot.last_snapshot_xmax = 9'452U;
    snapshot.last_snapshot_age = 8U;
    return snapshot;
}

bored::storage::DurabilityTelemetrySnapshot make_mock_durability()
{
    bored::storage::DurabilityTelemetrySnapshot snapshot{};
    snapshot.last_commit_lsn = 92'880U;
    snapshot.oldest_active_commit_lsn = 61'440U;
    snapshot.last_commit_segment_id = 6U;
    return snapshot;
}

void populate_mock_registry(StorageTelemetryRegistry& registry)
{
    registry.register_checkpoint_scheduler("mock_checkpoint", [] { return make_mock_checkpoint(); });
    registry.register_recovery("mock_recovery", [] { return make_mock_recovery(); });
    registry.register_transaction("mock_transactions", [] { return make_mock_transactions(); });
    registry.register_durability_horizon("mock_durability", [] { return make_mock_durability(); });
}

struct DiagnosticsContext final {
    StorageTelemetryRegistry* registry = nullptr;
    std::unique_ptr<StorageTelemetryRegistry> owned_registry{};
};

DiagnosticsContext resolve_registry(bool allow_mock)
{
    DiagnosticsContext context{};
    context.registry = bored::storage::get_global_storage_telemetry_registry();
    if (context.registry || !allow_mock) {
        return context;
    }
    auto mock_registry = std::make_unique<StorageTelemetryRegistry>();
    populate_mock_registry(*mock_registry);
    context.registry = mock_registry.get();
    context.owned_registry = std::move(mock_registry);
    return context;
}

void print_text_summary(const StorageDiagnosticsDocument& document, std::ostream& out)
{
    out << "Diagnostics schema v" << document.schema_version << " captured at "
        << format_timestamp(document.collected_at) << '\n';

    const auto& checkpoint = document.checkpoints.total;
    out << "Checkpoint:" << '\n';
    out << "  last checkpoint LSN : " << document.last_checkpoint_lsn << '\n';
    out << "  queue depth (last/max): " << checkpoint.last_queue_depth << " / "
        << checkpoint.max_queue_depth << '\n';
    out << "  blocked transactions  : " << checkpoint.blocked_transactions << '\n';
    out << "  last duration         : " << format_duration_ns(checkpoint.last_checkpoint_duration_ns)
        << " (max " << format_duration_ns(checkpoint.max_checkpoint_duration_ns) << ")" << '\n';

    const auto& recovery = document.recovery.total;
    out << "Recovery:" << '\n';
    out << "  backlog bytes         : " << format_bytes(document.outstanding_replay_backlog_bytes)
        << " (max " << format_bytes(recovery.max_replay_backlog_bytes) << ")" << '\n';
    out << "  plan invocations      : " << recovery.plan_invocations << " (failures "
        << recovery.plan_failures << ")" << '\n';
    out << "  redo/undo failures    : " << recovery.redo_failures << " / " << recovery.undo_failures
        << '\n';

    const auto& transactions = document.transactions.total;
    out << "Transactions:" << '\n';
    out << "  active                : " << transactions.active_transactions << '\n';
    out << "  committed / aborted   : " << transactions.committed_transactions << " / "
        << transactions.aborted_transactions << '\n';
    out << "  last snapshot age     : " << transactions.last_snapshot_age << '\n';

    const auto& durability = document.durability.total;
    out << "Durability:" << '\n';
    out << "  last commit LSN       : " << durability.last_commit_lsn << '\n';
    out << "  oldest active commit  : " << durability.oldest_active_commit_lsn << '\n';
    out << std::flush;
}

StorageDiagnosticsDocument capture_document(bool allow_mock)
{
    auto context = resolve_registry(allow_mock);
    if (!context.registry) {
        throw std::runtime_error("no storage telemetry registry available; run inside the server process or use --mock");
    }
    return bored::storage::collect_storage_diagnostics(*context.registry);
}

void write_text_summary(const StorageDiagnosticsDocument& document,
                        std::ostream& destination)
{
    print_text_summary(document, destination);
}

void capture_diagnostics(bool allow_mock,
                         const std::string& format,
                         const std::optional<std::filesystem::path>& output_path,
                         bool print_summary)
{
    const auto document = capture_document(allow_mock);

    std::ofstream file_stream;
    std::ostream* target = &std::cout;
    if (output_path) {
        file_stream.open(*output_path, std::ios::out | std::ios::trunc);
        if (!file_stream.is_open()) {
            throw std::runtime_error("failed to open output file: " + output_path->string());
        }
        target = &file_stream;
    }

    if (format == "json") {
        *target << bored::storage::storage_diagnostics_to_json(document) << '\n';
        if (print_summary) {
            if (output_path) {
                write_text_summary(document, std::cout);
            } else {
                write_text_summary(document, *target);
            }
        }
    } else if (format == "text") {
        write_text_summary(document, *target);
    } else {
        throw std::runtime_error("unsupported format: " + format);
    }
}

void export_catalog_snapshot(const std::optional<std::filesystem::path>& output_path)
{
    auto sampler = bored::catalog::get_global_catalog_introspection_sampler();
    if (!sampler) {
        throw std::runtime_error("no catalog introspection sampler available; run inside the server process");
    }

    const auto snapshot = sampler();
    const auto json = bored::catalog::catalog_introspection_to_json(snapshot);

    if (output_path) {
        std::ofstream file{*output_path, std::ios::out | std::ios::trunc};
        if (!file.is_open()) {
            throw std::runtime_error("failed to open output file: " + output_path->string());
        }
        file << json << '\n';
    } else {
        std::cout << json << '\n';
    }
}

void export_lock_snapshot(const std::optional<std::filesystem::path>& output_path)
{
    auto sampler = bored::storage::get_global_lock_snapshot_sampler();
    if (!sampler) {
        throw std::runtime_error("no lock snapshot sampler available; run inside the server process");
    }

    const auto document = bored::storage::collect_global_lock_diagnostics();
    const auto json = bored::storage::lock_diagnostics_to_json(document);

    if (output_path) {
        std::ofstream file{*output_path, std::ios::out | std::ios::trunc};
        if (!file.is_open()) {
            throw std::runtime_error("failed to open output file: " + output_path->string());
        }
        file << json << '\n';
    } else {
        std::cout << json << '\n';
    }
}

void export_metrics(bool allow_mock, const std::optional<std::filesystem::path>& output_path)
{
    auto context = resolve_registry(allow_mock);
    if (!context.registry) {
        throw std::runtime_error("no storage telemetry registry available; run inside the server process");
    }

    const auto metrics = bored::storage::collect_storage_metrics(*context.registry);
    const auto text = bored::storage::storage_metrics_to_openmetrics(metrics);

    if (output_path) {
        std::ofstream file{*output_path, std::ios::out | std::ios::trunc};
        if (!file.is_open()) {
            throw std::runtime_error("failed to open output file: " + output_path->string());
        }
        file << text;
    } else {
        std::cout << text;
    }
}

void print_repl_help()
{
    std::cout << "Commands:" << '\n';
    std::cout << "  capture [json|text] [--summary] [--mock|--live]  Capture diagnostics snapshot" << '\n';
    std::cout << "  help                                              Show this help" << '\n';
    std::cout << "  quit | exit                                       Leave the shell" << '\n';
}

void run_diagnostics_repl(bool allow_mock)
{
    replxx::Replxx repl;
    while (true) {
        const char* line = repl.input("diagnostics> ");
        if (line == nullptr) {
            std::cout << '\n';
            break;
        }

        std::string command = trim(line);
        if (command.empty()) {
            continue;
        }

        repl.history_add(command);
        auto tokens = split_tokens(command);
        if (tokens.empty()) {
            continue;
        }

        const auto& verb = tokens.front();
        if (verb == "quit" || verb == "exit") {
            break;
        }
        if (verb == "help") {
            print_repl_help();
            continue;
        }
        if (verb == "capture") {
            std::string format = "text";
            bool summary = false;
            bool mock = allow_mock;
            bool format_explicit = false;

            for (std::size_t index = 1U; index < tokens.size(); ++index) {
                const auto& token = tokens[index];
                if (token == "json") {
                    format = "json";
                    format_explicit = true;
                } else if (token == "text") {
                    format = "text";
                    format_explicit = true;
                } else if (token == "--summary") {
                    summary = true;
                } else if (token == "--mock") {
                    mock = true;
                } else if (token == "--live") {
                    mock = false;
                } else {
                    std::cout << "unknown capture option: " << token << '\n';
                }
            }

            try {
                capture_diagnostics(mock, format, std::nullopt, summary);
            } catch (const std::exception& error) {
                std::cout << "error: " << error.what() << '\n';
            }
            continue;
        }

        std::cout << "unrecognised command. Type 'help' for assistance." << '\n';
    }
}

}  // namespace

int main(int argc, char** argv)
{
    CLI::App app{"Operational tooling for the bored prototype"};
    app.require_subcommand(1);

    auto* diagnostics = app.add_subcommand("diagnostics", "Storage diagnostics commands");
    diagnostics->require_subcommand(1);

    bool capture_allow_mock = false;
    std::string capture_format = "json";
    std::string capture_output_path;
    bool capture_summary = false;

    auto* capture = diagnostics->add_subcommand("capture", "Capture a diagnostics snapshot");
    capture->add_flag("--mock", capture_allow_mock, "Allow mock telemetry when no live registry is present");
    capture->add_option("-f,--format", capture_format, "Output format (json or text)")
        ->transform(CLI::CheckedTransformer({{"json", "json"}, {"text", "text"}}));
    capture->add_option("-o,--output", capture_output_path, "Write output to a file instead of stdout");
    capture->add_flag("--summary", capture_summary, "Emit a human-readable summary alongside JSON output");
    capture->callback([&]() {
        std::optional<std::filesystem::path> output_path;
        if (!capture_output_path.empty()) {
            output_path = std::filesystem::path(capture_output_path);
        }
        capture_diagnostics(capture_allow_mock, capture_format, output_path, capture_summary);
    });

    std::string catalog_output_path;
    auto* catalog = diagnostics->add_subcommand("catalog", "Export catalog system view snapshot as JSON");
    catalog->add_option("-o,--output", catalog_output_path, "Write output to a file instead of stdout");
    catalog->callback([&]() {
        std::optional<std::filesystem::path> output_path;
        if (!catalog_output_path.empty()) {
            output_path = std::filesystem::path(catalog_output_path);
        }
        export_catalog_snapshot(output_path);
    });

    std::string locks_output_path;
    auto* locks = diagnostics->add_subcommand("locks", "Export active page lock snapshot as JSON");
    locks->add_option("-o,--output", locks_output_path, "Write output to a file instead of stdout");
    locks->callback([&]() {
        std::optional<std::filesystem::path> output_path;
        if (!locks_output_path.empty()) {
            output_path = std::filesystem::path(locks_output_path);
        }
        export_lock_snapshot(output_path);
    });

    bool metrics_allow_mock = false;
    std::string metrics_output_path;
    auto* metrics = diagnostics->add_subcommand("metrics", "Export storage metrics in OpenMetrics format");
    metrics->add_flag("--mock", metrics_allow_mock, "Allow mock telemetry when no live registry is present");
    metrics->add_option("-o,--output", metrics_output_path, "Write output to a file instead of stdout");
    metrics->callback([&]() {
        std::optional<std::filesystem::path> output_path;
        if (!metrics_output_path.empty()) {
            output_path = std::filesystem::path(metrics_output_path);
        }
        export_metrics(metrics_allow_mock, output_path);
    });

    bool repl_live_only = false;
    auto* repl = diagnostics->add_subcommand("repl", "Start an interactive diagnostics shell");
    repl->add_flag("--live", repl_live_only, "Require a live registry (disable mock fallback)");
    repl->callback([&]() {
        run_diagnostics_repl(!repl_live_only);
    });

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return app.exit(error);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
