#include "bored/storage/vacuum_background_loop.hpp"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <utility>

using namespace std::chrono_literals;

namespace bored::storage::tests {

namespace {

VacuumWorker::Config make_worker_config()
{
    VacuumWorker::Config config{};
    config.scheduler.min_interval = 0ms;
    config.tick_interval = 5ms;
    return config;
}

VacuumWorker::SafeHorizonProvider make_safe_horizon_provider()
{
    return [](std::uint64_t& horizon) -> std::error_code {
        horizon = 100U;
        return {};
    };
}

}  // namespace

TEST_CASE("VacuumBackgroundLoop runs scheduled work", "[vacuum]")
{
    std::promise<void> first_dispatch;
    std::atomic_bool first_signalled{false};
    std::atomic<std::size_t> dispatch_count{0U};

    auto dispatch = [&first_dispatch, &first_signalled, &dispatch_count](std::span<const VacuumWorkItem> batch) -> std::error_code {
        dispatch_count.fetch_add(1U, std::memory_order_relaxed);
        if (!batch.empty() && !first_signalled.exchange(true)) {
            first_dispatch.set_value();
        }
        return {};
    };

    auto config = make_worker_config();
    config.tick_interval = 5s;
    VacuumBackgroundLoop loop{std::move(config), make_safe_horizon_provider(), dispatch};
    loop.start();
    loop.schedule_page(33U, 10U);
    loop.request_force_run();

    const auto status = first_dispatch.get_future().wait_for(200ms);
    REQUIRE(status == std::future_status::ready);
    CHECK(dispatch_count.load(std::memory_order_relaxed) >= 1U);

    loop.stop();
    CHECK_FALSE(loop.running());
}

TEST_CASE("VacuumBackgroundLoop retries after failure", "[vacuum]")
{
    std::atomic_bool should_fail{true};
    const auto expected_error = std::make_error_code(std::errc::io_error);
    std::promise<void> success_dispatch;
    std::atomic_bool success_signalled{false};

    auto dispatch = [&should_fail, expected_error, &success_dispatch, &success_signalled](std::span<const VacuumWorkItem> batch) -> std::error_code {
        if (should_fail.exchange(false)) {
            return expected_error;
        }
        if (!batch.empty() && !success_signalled.exchange(true)) {
            success_dispatch.set_value();
        }
        return {};
    };

    auto config = make_worker_config();
    config.tick_interval = 1s;
    VacuumBackgroundLoop loop{std::move(config), make_safe_horizon_provider(), dispatch};
    loop.start();
    loop.schedule_page(84U, 20U);
    loop.request_force_run();

    // First run reports failure.
    for (int attempt = 0; attempt < 20 && should_fail.load(std::memory_order_acquire); ++attempt) {
        std::this_thread::sleep_for(5ms);
    }
    REQUIRE_FALSE(should_fail.load(std::memory_order_acquire));
    auto telemetry = loop.telemetry_snapshot();
    for (int attempt = 0; attempt < 200 && telemetry.dispatch_failures == 0U; ++attempt) {
        std::this_thread::sleep_for(5ms);
        telemetry = loop.telemetry_snapshot();
    }
    REQUIRE(telemetry.dispatch_failures == 1U);
    auto last_error = loop.last_error();
    for (int attempt = 0; attempt < 200 && !last_error.has_value(); ++attempt) {
        std::this_thread::sleep_for(5ms);
        last_error = loop.last_error();
    }
    CHECK(last_error.has_value());
    if (last_error.has_value()) {
        CHECK(*last_error == expected_error);
    }

    // Successful retry arrives on the next tick.
    loop.request_force_run();

    const auto status = success_dispatch.get_future().wait_for(500ms);
    REQUIRE(status == std::future_status::ready);
    last_error = loop.last_error();
    for (int attempt = 0; attempt < 200 && last_error.has_value(); ++attempt) {
        std::this_thread::sleep_for(5ms);
        last_error = loop.last_error();
    }
    CHECK_FALSE(last_error.has_value());

    loop.stop();
}

}  // namespace bored::storage::tests
