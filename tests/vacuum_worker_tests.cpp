#include "bored/storage/vacuum_worker.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <system_error>
#include <vector>

using namespace std::chrono_literals;

namespace bored::storage::tests {

TEST_CASE("VacuumWorker dispatches ready pages", "[vacuum]")
{
    VacuumWorker::Config config{};
    config.scheduler.min_interval = 0ms;
    config.tick_interval = 1ms;

    std::vector<VacuumWorkItem> dispatched{};

    VacuumWorker worker{
        config,
        [](std::uint64_t& horizon) -> std::error_code {
            horizon = 100U;
            return {};
        },
        [&dispatched](std::span<const VacuumWorkItem> batch) -> std::error_code {
            dispatched.assign(batch.begin(), batch.end());
            return {};
        }};

    worker.schedule_page(42U, 50U);

    const auto ec = worker.run_once(std::chrono::steady_clock::now(), true);
    REQUIRE_FALSE(ec);
    REQUIRE(dispatched.size() == 1U);
    CHECK(dispatched.front().page_id == 42U);
    CHECK(dispatched.front().prune_lsn == 50U);

    const auto telemetry = worker.telemetry_snapshot();
    CHECK(telemetry.pages_dispatched == 1U);
    CHECK(telemetry.batches_dispatched == 1U);
    CHECK(telemetry.pending_pages == 0U);
    CHECK_FALSE(worker.last_error().has_value());
    CHECK(worker.tick_interval() == 1ms);
}

TEST_CASE("VacuumWorker surfaces dispatch failures and retries", "[vacuum]")
{
    VacuumWorker::Config config{};
    config.scheduler.min_interval = 0ms;

    std::atomic_bool should_fail{true};
    const auto dispatch_error = std::make_error_code(std::errc::io_error);

    VacuumWorker worker{
        config,
        [](std::uint64_t& horizon) -> std::error_code {
            horizon = 100U;
            return {};
        },
        [&should_fail, dispatch_error](std::span<const VacuumWorkItem> batch) -> std::error_code {
            if (should_fail.load()) {
                return dispatch_error;
            }
            return batch.empty() ? std::make_error_code(std::errc::invalid_argument) : std::error_code{};
        }};

    worker.schedule_page(7U, 10U);

    auto ec = worker.run_once(std::chrono::steady_clock::now(), true);
    REQUIRE(ec == dispatch_error);

    auto last_error = worker.last_error();
    REQUIRE(last_error.has_value());
    CHECK(*last_error == dispatch_error);

    should_fail.store(false);
    ec = worker.run_once(std::chrono::steady_clock::now(), true);
    REQUIRE_FALSE(ec);

    const auto telemetry = worker.telemetry_snapshot();
    CHECK(telemetry.dispatch_failures == 1U);
    CHECK(telemetry.batches_dispatched == 1U);
    CHECK(telemetry.pages_dispatched == 1U);
    CHECK(telemetry.pending_pages == 0U);
}

}  // namespace bored::storage::tests
