#include "bored/storage/index_retention.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <system_error>
#include <vector>

using namespace std::chrono_literals;
using bored::storage::IndexRetentionCandidate;
using bored::storage::IndexRetentionManager;
using bored::storage::IndexRetentionStats;
using bored::storage::IndexRetentionTelemetrySnapshot;

TEST_CASE("IndexRetentionManager dispatches eligible candidates after checkpoint")
{
    std::vector<IndexRetentionCandidate> dispatched;
    auto dispatch = [&](std::span<const IndexRetentionCandidate> span, IndexRetentionStats& stats) {
        dispatched.insert(dispatched.end(), span.begin(), span.end());
        stats.pruned_candidates += span.size();
        return std::error_code{};
    };

    IndexRetentionManager::Config config{};
    config.retention_window = 0ms;
    config.max_dispatch_batch = 8U;

    IndexRetentionManager manager{config, dispatch};

    IndexRetentionCandidate candidate{};
    candidate.index_id = 101U;
    candidate.page_id = 42U;
    candidate.level = 1U;
    candidate.prune_lsn = 1234U;

    manager.schedule_candidate(candidate);

    IndexRetentionStats stats{};
    const auto now = std::chrono::steady_clock::now();
    REQUIRE_FALSE(manager.apply_checkpoint(now, 2000U, &stats));

    REQUIRE(dispatched.size() == 1U);
    CHECK(dispatched.front().index_id == candidate.index_id);
    CHECK(dispatched.front().page_id == candidate.page_id);
    CHECK(stats.dispatched_candidates == 1U);
    CHECK(stats.pruned_candidates == 1U);

    const auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.dispatched_candidates == 1U);
    CHECK(telemetry.pruned_candidates == 1U);
}

TEST_CASE("IndexRetentionManager enforces retention window before dispatch")
{
    IndexRetentionManager::Config config{};
    config.retention_window = std::chrono::hours{1};
    config.min_checkpoint_interval = 0ms;

    IndexRetentionManager manager{config};

    IndexRetentionCandidate recent{};
    recent.index_id = 7U;
    recent.page_id = 11U;
    recent.level = 0U;
    recent.prune_lsn = 900U;
    recent.scheduled_at = std::chrono::steady_clock::now();
    manager.schedule_candidate(recent);

    IndexRetentionStats first_stats{};
    REQUIRE_FALSE(manager.apply_checkpoint(std::chrono::steady_clock::now(), 1200U, &first_stats));
    CHECK(first_stats.dispatched_candidates == 0U);
    CHECK(first_stats.pruned_candidates == 0U);

    IndexRetentionCandidate stale{};
    stale.index_id = 7U;
    stale.page_id = 12U;
    stale.level = 0U;
    stale.prune_lsn = 950U;
    stale.scheduled_at = std::chrono::steady_clock::now() - std::chrono::hours{2};
    manager.schedule_candidate(stale);

    IndexRetentionStats second_stats{};
    REQUIRE_FALSE(manager.apply_checkpoint(std::chrono::steady_clock::now(), 2000U, &second_stats));
    CHECK(second_stats.dispatched_candidates == 0U);
    CHECK(second_stats.pruned_candidates == 0U);  // no dispatch hook defined

    const auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.scheduled_candidates >= 2U);
    CHECK(telemetry.skipped_candidates >= 1U);
}

TEST_CASE("IndexRetentionManager respects pending candidate limit")
{
    IndexRetentionManager::Config config{};
    config.max_pending_candidates = 1U;

    IndexRetentionManager manager{config};

    IndexRetentionCandidate first{};
    first.index_id = 1U;
    first.page_id = 1U;
    first.level = 0U;
    first.prune_lsn = 100U;
    manager.schedule_candidate(first);

    IndexRetentionCandidate second = first;
    second.page_id = 2U;
    manager.schedule_candidate(second);

    const auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.scheduled_candidates == 1U);
    CHECK(telemetry.dropped_candidates == 1U);
    CHECK(telemetry.pending_candidates == 1U);
}

TEST_CASE("IndexRetentionManager reinserts candidates when dispatch fails")
{
    bool should_fail = true;
    auto dispatch = [&](std::span<const IndexRetentionCandidate> span, IndexRetentionStats&) {
        if (should_fail) {
            should_fail = false;
            return std::make_error_code(std::errc::io_error);
        }
        return std::error_code{};
    };

    IndexRetentionManager::Config config{};
    config.retention_window = 0ms;
    config.min_checkpoint_interval = 0ms;

    IndexRetentionManager manager{config, dispatch};

    IndexRetentionCandidate candidate{};
    candidate.index_id = 55U;
    candidate.page_id = 99U;
    candidate.level = 2U;
    candidate.prune_lsn = 777U;
    manager.schedule_candidate(candidate);

    IndexRetentionStats stats{};
    const auto now = std::chrono::steady_clock::now();
    auto first = manager.apply_checkpoint(now, 900U, &stats);
    REQUIRE(first);

    REQUIRE_FALSE(manager.apply_checkpoint(now + 1ms, 900U, &stats));
    const auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.dispatch_failures == 1U);
    CHECK(telemetry.dispatched_candidates == 1U);
    CHECK(telemetry.pending_candidates == 0U);
}
