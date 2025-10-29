#include "bored/storage/index_retention_pruner.hpp"

#include <utility>

namespace bored::storage {

IndexRetentionPruner::IndexRetentionPruner() = default;

IndexRetentionPruner::IndexRetentionPruner(Config config)
    : config_{std::move(config)}
{
    if (config_.state_retention < std::chrono::milliseconds::zero()) {
        config_.state_retention = std::chrono::milliseconds::zero();
    }
}

std::error_code IndexRetentionPruner::dispatch(std::span<const IndexRetentionCandidate> candidates,
                                               IndexRetentionStats& stats)
{
    if (candidates.empty()) {
        return {};
    }

    const auto now = std::chrono::steady_clock::now();
    const auto cached_state_retention = config_.state_retention;
    evict_stale(now);

    for (const auto& candidate : candidates) {
        const CandidateKey key{candidate.index_id, candidate.page_id, candidate.level};

        CandidateState previous_state{};
        bool should_prune = false;
        bool was_stale = false;

        {
            std::lock_guard guard{mutex_};
            auto& state = states_[key];
            previous_state = state;
            if (cached_state_retention > std::chrono::milliseconds::zero()
                && state.last_seen_at != std::chrono::steady_clock::time_point{}
                && (now - state.last_seen_at) >= cached_state_retention) {
                state = CandidateState{};
                was_stale = true;
            }

            state.last_seen_at = now;
            if (state.last_prune_lsn < candidate.prune_lsn) {
                state.last_prune_lsn = candidate.prune_lsn;
                state.last_pruned_at = now;
                should_prune = true;
            } else if (was_stale) {
                state.last_prune_lsn = candidate.prune_lsn;
                state.last_pruned_at = now;
                should_prune = true;
            } else if (cached_state_retention == std::chrono::milliseconds::zero()) {
                should_prune = true;
            } else if ((now - state.last_pruned_at) >= cached_state_retention) {
                state.last_prune_lsn = candidate.prune_lsn;
                state.last_pruned_at = now;
                should_prune = true;
            }
        }

        if (!should_prune) {
            stats.skipped_candidates += 1;
            continue;
        }

        if (config_.prune_callback) {
            if (auto ec = config_.prune_callback(candidate); ec) {
                std::lock_guard guard{mutex_};
                auto it = states_.find(key);
                if (it != states_.end()) {
                    it->second = previous_state;
                    it->second.last_seen_at = now;
                }
                return ec;
            }
        }

        stats.pruned_candidates += 1;
    }

    return {};
}

void IndexRetentionPruner::reset_state()
{
    std::lock_guard guard{mutex_};
    states_.clear();
}

void IndexRetentionPruner::evict_stale(std::chrono::steady_clock::time_point now)
{
    if (config_.state_retention <= std::chrono::milliseconds::zero()) {
        return;
    }

    std::lock_guard guard{mutex_};
    for (auto it = states_.begin(); it != states_.end();) {
        if (it->second.last_seen_at == std::chrono::steady_clock::time_point{}) {
            ++it;
            continue;
        }

        if (now - it->second.last_seen_at >= config_.state_retention) {
            it = states_.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace bored::storage
