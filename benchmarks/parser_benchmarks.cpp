#include "bored/parser/grammar.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string_view>

using namespace std::chrono_literals;

namespace
{

using Clock = std::chrono::steady_clock;

struct Scenario final {
    std::string_view name;
    std::string_view script;
};

constexpr std::string_view schema_script = R"(CREATE DATABASE analytics;
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION admin;
CREATE TABLE IF NOT EXISTS analytics.events (
    event_id BIGINT PRIMARY KEY,
    visitor_id BIGINT NOT NULL,
    payload JSON DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS analytics.sessions (
    session_id UUID PRIMARY KEY,
    visitor_id BIGINT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE VIEW analytics.active_sessions AS
SELECT s.session_id, s.visitor_id
FROM analytics.sessions s
WHERE s.last_seen > CURRENT_TIMESTAMP - INTERVAL '15 minutes';
)";

constexpr std::string_view maintenance_script = R"(DROP TABLE IF EXISTS analytics.sessions;
DROP TABLE IF EXISTS analytics.events;
DROP SCHEMA IF EXISTS analytics CASCADE;
)";

constexpr std::array scenarios{
    Scenario{"bootstrap", schema_script},
    Scenario{"teardown", maintenance_script},
};

std::size_t parse_iterations_from_args(int argc, char** argv, std::size_t default_iterations)
{
    for (int index = 1; index < argc; ++index) {
        std::string_view arg{argv[index]};
        if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: bored_benchmarks [--iterations N]\n";
            std::exit(EXIT_SUCCESS);
        }
        if ((arg == "--iterations" || arg == "-n") && index + 1 < argc) {
            const auto value = std::strtoull(argv[index + 1], nullptr, 10);
            if (value > 0U) {
                return static_cast<std::size_t>(value);
            }
        }
    }

    return default_iterations;
}

struct BenchmarkSummary final {
    std::size_t iterations = 0U;
    std::size_t statements = 0U;
    std::size_t diagnostics = 0U;
    std::size_t unsuccessful = 0U;
    Clock::duration elapsed{};
};

BenchmarkSummary run_scenario(const Scenario& scenario, std::size_t iterations)
{
    BenchmarkSummary summary{};
    summary.iterations = iterations;

    const auto start = Clock::now();
    for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
        auto result = bored::parser::parse_ddl_script(scenario.script);
        summary.statements += result.statements.size();
        for (const auto& statement : result.statements) {
            summary.diagnostics += statement.diagnostics.size();
            if (!statement.success) {
                ++summary.unsuccessful;
            }
        }
    }
    const auto stop = Clock::now();
    summary.elapsed = stop - start;

    return summary;
}

void report_summary(const Scenario& scenario, const BenchmarkSummary& summary)
{
    const auto seconds = std::chrono::duration<double>(summary.elapsed).count();
    const auto scripts_per_second = seconds > 0.0 ? static_cast<double>(summary.iterations) / seconds : 0.0;
    const auto statements_per_second = seconds > 0.0 ? static_cast<double>(summary.statements) / seconds : 0.0;
    const auto diagnostics_per_script = summary.iterations > 0U
                                            ? static_cast<double>(summary.diagnostics) / summary.iterations
                                            : 0.0;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Scenario: " << scenario.name << "\n";
    std::cout << "  Scripts: " << summary.iterations << "\n";
    std::cout << "  Statements/script: "
              << (summary.iterations > 0U ? static_cast<double>(summary.statements) / summary.iterations : 0.0)
              << "\n";
    std::cout << "  Diagnostics/script: " << diagnostics_per_script << "\n";
    std::cout << "  Elapsed: " << seconds << " s\n";
    std::cout << "  Scripts/s: " << scripts_per_second << "\n";
    std::cout << "  Statements/s: " << statements_per_second << "\n";
    if (summary.unsuccessful > 0U) {
        std::cout << "  Unsuccessful statements: " << summary.unsuccessful << "\n";
    }
    std::cout << std::defaultfloat;
}

}  // namespace

int main(int argc, char** argv)
{
    constexpr std::size_t default_iterations = 1000U;
    const auto iterations = parse_iterations_from_args(argc, argv, default_iterations);

    for (const auto& scenario : scenarios) {
        auto summary = run_scenario(scenario, iterations);
        report_summary(scenario, summary);
    }

    return 0;
}
