#include "bored/storage/index_retention_pruner.hpp"

#include <array>
#include <chrono>
#include <thread>
#include <vector>

#include <catch2/catch_test_macros.hpp>

using namespace std::chrono_literals;

namespace bored::storage::tests {

TEST_CASE("IndexRetentionPruner prunes unique candidates", "[storage][index_retention]")
{
    std::vector<IndexRetentionCandidate> invoked{};
    IndexRetentionPruner::Config config{};
    config.prune_callback = [&invoked](const IndexRetentionCandidate& candidate) -> std::error_code {
        invoked.push_back(candidate);
        return {};
    };

    IndexRetentionPruner pruner{config};

    std::array<IndexRetentionCandidate, 3> candidates{
        IndexRetentionCandidate{.index_id = 1U, .page_id = 42U, .level = 0U, .prune_lsn = 10U, .scheduled_at = std::chrono::steady_clock::now()},
        IndexRetentionCandidate{.index_id = 1U, .page_id = 43U, .level = 0U, .prune_lsn = 8U, .scheduled_at = std::chrono::steady_clock::now()},
        IndexRetentionCandidate{.index_id = 2U, .page_id = 99U, .level = 1U, .prune_lsn = 15U, .scheduled_at = std::chrono::steady_clock::now()},
    };

    IndexRetentionStats stats{};
    REQUIRE_FALSE(pruner.dispatch(candidates, stats));
    REQUIRE(stats.pruned_candidates == 3U);
    REQUIRE(stats.skipped_candidates == 0U);
    REQUIRE(invoked.size() == 3U);

    std::array<IndexRetentionCandidate, 1> repeat{candidates[0]};
    IndexRetentionStats repeat_stats{};
    REQUIRE_FALSE(pruner.dispatch(repeat, repeat_stats));
    REQUIRE(repeat_stats.pruned_candidates == 0U);
    REQUIRE(repeat_stats.skipped_candidates == 1U);
    REQUIRE(invoked.size() == 3U);

    std::array<IndexRetentionCandidate, 1> newer{candidates[0]};
    newer[0].prune_lsn = 11U;
    IndexRetentionStats newer_stats{};
    REQUIRE_FALSE(pruner.dispatch(newer, newer_stats));
    REQUIRE(newer_stats.pruned_candidates == 1U);
    REQUIRE(invoked.size() == 4U);
}

TEST_CASE("IndexRetentionPruner evicts stale state", "[storage][index_retention]")
{
    std::vector<IndexRetentionCandidate> invoked{};
    IndexRetentionPruner::Config config{};
    config.state_retention = 10ms;
    config.prune_callback = [&invoked](const IndexRetentionCandidate& candidate) -> std::error_code {
        invoked.push_back(candidate);
        return {};
    };

    IndexRetentionPruner pruner{config};

    IndexRetentionCandidate candidate{
        .index_id = 5U,
        .page_id = 21U,
        .level = 0U,
        .prune_lsn = 4U,
        .scheduled_at = std::chrono::steady_clock::now(),
    };

    std::array<IndexRetentionCandidate, 1> initial{candidate};
    IndexRetentionStats initial_stats{};
    REQUIRE_FALSE(pruner.dispatch(initial, initial_stats));
    REQUIRE(initial_stats.pruned_candidates == 1U);
    REQUIRE(invoked.size() == 1U);

    std::this_thread::sleep_for(20ms);

    std::array<IndexRetentionCandidate, 1> second{candidate};
    IndexRetentionStats second_stats{};
    REQUIRE_FALSE(pruner.dispatch(second, second_stats));
    CAPTURE(second_stats.pruned_candidates);
    CAPTURE(second_stats.skipped_candidates);
    REQUIRE(second_stats.pruned_candidates == 1U);
    REQUIRE(invoked.size() == 2U);
}

}  // namespace bored::storage::tests
