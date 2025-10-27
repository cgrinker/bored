#include "bored/storage/vacuum_scheduler.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <system_error>
#include <vector>

using bored::storage::VacuumScheduler;
using bored::storage::VacuumWorkItem;

namespace {

VacuumScheduler make_scheduler(std::size_t max_batch, std::size_t max_pending, std::chrono::milliseconds interval = std::chrono::milliseconds{0})
{
    VacuumScheduler::Config config{};
    config.min_interval = interval;
    config.max_batch_pages = max_batch;
    config.max_pending_pages = max_pending;
    return VacuumScheduler{config};
}

}  // namespace

TEST_CASE("VacuumScheduler dispatches ready pages")
{
    auto scheduler = make_scheduler(2U, 8U);

    scheduler.schedule_page(10U, 50U);
    scheduler.schedule_page(20U, 125U);

    std::uint64_t horizon = 80U;
    std::vector<VacuumWorkItem> dispatched;

    const auto horizon_provider = [&horizon](std::uint64_t& out) -> std::error_code {
        out = horizon;
        return {};
    };

    const auto dispatch = [&dispatched](std::span<const VacuumWorkItem> batch) -> std::error_code {
        dispatched.assign(batch.begin(), batch.end());
        return {};
    };

    REQUIRE_FALSE(scheduler.maybe_run(std::chrono::steady_clock::now(), horizon_provider, dispatch));
    REQUIRE(dispatched.size() == 1U);
    CHECK(dispatched.front().page_id == 10U);
    CHECK(dispatched.front().prune_lsn == 50U);
    CHECK(scheduler.pending_pages() == 1U);

    const auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.runs == 1U);
    CHECK(telemetry.pages_dispatched == 1U);
    CHECK(telemetry.pending_pages == 1U);
}

TEST_CASE("VacuumScheduler enforces minimum interval between runs")
{
    auto scheduler = make_scheduler(4U, 8U, std::chrono::milliseconds{5});
    scheduler.schedule_page(33U, 100U);

    std::uint64_t horizon = 200U;
    std::vector<VacuumWorkItem> dispatched;

    const auto horizon_provider = [&horizon](std::uint64_t& out) -> std::error_code {
        out = horizon;
        return {};
    };

    auto dispatch = [&dispatched](std::span<const VacuumWorkItem> batch) -> std::error_code {
        dispatched.assign(batch.begin(), batch.end());
        return {};
    };

    const auto now = std::chrono::steady_clock::now();
    REQUIRE_FALSE(scheduler.maybe_run(now, horizon_provider, dispatch));
    REQUIRE(dispatched.size() == 1U);

    dispatched.clear();
    REQUIRE_FALSE(scheduler.maybe_run(now + std::chrono::milliseconds{1}, horizon_provider, dispatch));
    CHECK(dispatched.empty());

    auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.runs == 1U);
    CHECK(telemetry.skipped_runs >= 1U);

    REQUIRE_FALSE(scheduler.maybe_run(now + std::chrono::milliseconds{6}, horizon_provider, dispatch));
    CHECK(dispatched.empty());
}

TEST_CASE("VacuumScheduler requeues on dispatch failure")
{
    auto scheduler = make_scheduler(4U, 8U);
    scheduler.schedule_page(44U, 10U);

    std::uint64_t horizon = 20U;
    const auto horizon_provider = [&horizon](std::uint64_t& out) -> std::error_code {
        out = horizon;
        return {};
    };

    bool fail_dispatch = true;
    const auto dispatch = [&fail_dispatch](std::span<const VacuumWorkItem>) -> std::error_code {
        if (fail_dispatch) {
            return std::make_error_code(std::errc::io_error);
        }
        return {};
    };

    auto now = std::chrono::steady_clock::now();
    const auto failure = scheduler.maybe_run(now, horizon_provider, dispatch);
    REQUIRE(failure);
    CHECK(failure.value() == static_cast<int>(std::errc::io_error));
    CHECK(scheduler.pending_pages() == 1U);

    fail_dispatch = false;
    REQUIRE_FALSE(scheduler.maybe_run(now + std::chrono::milliseconds{1}, horizon_provider, dispatch));
    CHECK(scheduler.pending_pages() == 0U);

    const auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.dispatch_failures == 1U);
    CHECK(telemetry.runs == 1U);
}

TEST_CASE("VacuumScheduler deduplicates pages and enforces capacity")
{
    VacuumScheduler::Config config{};
    config.min_interval = std::chrono::milliseconds{0};
    config.max_batch_pages = 4U;
    config.max_pending_pages = 1U;
    VacuumScheduler scheduler{config};

    scheduler.schedule_page(77U, 400U);
    scheduler.schedule_page(77U, 200U);

    CHECK(scheduler.pending_pages() == 1U);

    scheduler.schedule_page(88U, 100U);

    const auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.dropped_pages == 1U);
    CHECK(scheduler.pending_pages() == 1U);

    std::uint64_t horizon = 500U;
    const auto horizon_provider = [&horizon](std::uint64_t& out) -> std::error_code {
        out = horizon;
        return {};
    };

    std::vector<VacuumWorkItem> dispatched;
    const auto dispatch = [&dispatched](std::span<const VacuumWorkItem> batch) -> std::error_code {
        dispatched.assign(batch.begin(), batch.end());
        return {};
    };

    REQUIRE_FALSE(scheduler.maybe_run(std::chrono::steady_clock::now(), horizon_provider, dispatch));
    REQUIRE(dispatched.size() == 1U);
    CHECK(dispatched.front().page_id == 77U);
    CHECK(dispatched.front().prune_lsn == 200U);
}
