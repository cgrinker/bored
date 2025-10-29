#include "bored/storage/index_retention.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace bored::storage {

namespace {

[[nodiscard]] std::chrono::steady_clock::time_point steady_now()
{
    return std::chrono::steady_clock::now();
}

[[nodiscard]] bool is_within_window(const IndexRetentionManager::Config& config,
                                    const IndexRetentionCandidate& candidate,
                                    std::chrono::steady_clock::time_point reference)
{
    if (config.retention_window.count() <= 0) {
        return true;
    }
    const auto scheduled = candidate.scheduled_at;
    if (scheduled == std::chrono::steady_clock::time_point{}) {
        return true;
    }
    const auto threshold = reference - config.retention_window;
    return scheduled <= threshold;
}

}  // namespace

IndexRetentionManager::IndexRetentionManager()
    : IndexRetentionManager(Config{}, DispatchHook{})
{
}

IndexRetentionManager::IndexRetentionManager(Config config)
    : IndexRetentionManager(std::move(config), DispatchHook{})
{
}

IndexRetentionManager::IndexRetentionManager(Config config, DispatchHook dispatch)
    : config_{std::move(config)}
    , dispatch_{std::move(dispatch)}
{
    register_telemetry();
}

IndexRetentionManager::~IndexRetentionManager()
{
    unregister_telemetry();
}

void IndexRetentionManager::register_telemetry()
{
    if (!config_.telemetry_registry || config_.telemetry_identifier.empty()) {
        return;
    }
    config_.telemetry_registry->register_index_retention(config_.telemetry_identifier, [this] {
        return this->telemetry_snapshot();
    });
}

void IndexRetentionManager::unregister_telemetry()
{
    if (!config_.telemetry_registry || config_.telemetry_identifier.empty()) {
        return;
    }
    config_.telemetry_registry->unregister_index_retention(config_.telemetry_identifier);
}

std::chrono::steady_clock::time_point IndexRetentionManager::now() const noexcept
{
    return steady_now();
}

void IndexRetentionManager::schedule_candidate(IndexRetentionCandidate candidate)
{
    if (candidate.index_id == 0U) {
        return;
    }

    if (candidate.scheduled_at == std::chrono::steady_clock::time_point{}) {
        candidate.scheduled_at = now();
    }

    const CandidateKey key{candidate.index_id, candidate.page_id, candidate.level};

    std::lock_guard guard{mutex_};
    auto iter = pending_.find(key);
    if (iter != pending_.end()) {
        if (candidate.prune_lsn > iter->second.prune_lsn) {
            iter->second.prune_lsn = candidate.prune_lsn;
        }
        iter->second.scheduled_at = candidate.scheduled_at;
        return;
    }

    if (pending_.size() >= config_.max_pending_candidates) {
        telemetry_.dropped_candidates += 1U;
        return;
    }

    pending_.emplace(key, candidate);
    telemetry_.scheduled_candidates += 1U;
    telemetry_.pending_candidates = pending_.size();
}

void IndexRetentionManager::clear_index(std::uint64_t index_id)
{
    if (index_id == 0U) {
        return;
    }

    std::lock_guard guard{mutex_};
    for (auto it = pending_.begin(); it != pending_.end();) {
        if (it->first.index_id == index_id) {
            it = pending_.erase(it);
        } else {
            ++it;
        }
    }
    telemetry_.pending_candidates = pending_.size();
}

void IndexRetentionManager::reset()
{
    std::lock_guard guard{mutex_};
    pending_.clear();
    telemetry_ = {};
    last_checkpoint_time_ = {};
    has_checkpoint_time_ = false;
}

std::error_code IndexRetentionManager::apply_checkpoint(std::chrono::steady_clock::time_point when,
                                                        std::uint64_t checkpoint_lsn,
                                                        IndexRetentionStats* stats)
{
    const auto run_start = steady_now();

    std::vector<CandidateEntry> selected;
    selected.reserve(pending_.size());

    std::uint64_t scanned_candidates = 0U;

    {
        std::lock_guard guard{mutex_};

        if (config_.min_checkpoint_interval.count() > 0 && has_checkpoint_time_) {
            const auto elapsed = when - last_checkpoint_time_;
            if (elapsed < config_.min_checkpoint_interval) {
                return {};
            }
        }

        std::size_t original_size = pending_.size();
        selected.reserve(original_size);

        for (auto it = pending_.begin(); it != pending_.end();) {
            ++scanned_candidates;

            const auto& candidate = it->second;
            if (candidate.prune_lsn == 0U || candidate.prune_lsn > checkpoint_lsn) {
                ++it;
                continue;
            }

            if (!is_within_window(config_, candidate, when)) {
                ++it;
                continue;
            }

            selected.emplace_back(*it);
            it = pending_.erase(it);
        }

        telemetry_.checkpoint_runs += 1U;
        telemetry_.pending_candidates = pending_.size();
        last_checkpoint_time_ = when;
        has_checkpoint_time_ = true;
    }

    const auto skipped_candidates = scanned_candidates >= selected.size()
        ? scanned_candidates - static_cast<std::uint64_t>(selected.size())
        : 0U;

    if (stats) {
        stats->scanned_candidates += scanned_candidates;
    }

    const auto run_end_if_empty = steady_now();

    if (selected.empty()) {
        const auto run_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(run_end_if_empty - run_start);
        const auto duration_ns = run_ns.count() > 0 ? static_cast<std::uint64_t>(run_ns.count()) : 0ULL;
        std::lock_guard guard{mutex_};
        telemetry_.total_checkpoint_duration_ns += duration_ns;
        telemetry_.last_checkpoint_duration_ns = duration_ns;
        telemetry_.skipped_candidates += skipped_candidates;
        return {};
    }

    if (stats) {
        stats->skipped_candidates += skipped_candidates;
    }

    std::vector<IndexRetentionCandidate> batches;
    batches.reserve(selected.size());
    for (const auto& entry : selected) {
        batches.push_back(entry.second);
    }

    const auto dispatch_start = steady_now();

    if (!dispatch_) {
        if (stats) {
            stats->skipped_candidates += static_cast<std::uint64_t>(batches.size());
        }

        const auto dispatch_end = steady_now();
        const auto run_end = dispatch_end;
        const auto run_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(run_end - run_start);
        const auto run_duration_ns = run_ns.count() > 0 ? static_cast<std::uint64_t>(run_ns.count()) : 0ULL;

        {
            std::lock_guard guard{mutex_};
            telemetry_.skipped_candidates += skipped_candidates + static_cast<std::uint64_t>(batches.size());
            telemetry_.total_checkpoint_duration_ns += run_duration_ns;
            telemetry_.last_checkpoint_duration_ns = run_duration_ns;
            telemetry_.pending_candidates = pending_.size();
        }

        return {};
    }

    IndexRetentionStats aggregate_stats{};
    aggregate_stats.scanned_candidates = scanned_candidates;

    std::size_t offset = 0U;
    std::uint64_t batch_count = 0U;

    while (offset < batches.size()) {
        const auto remaining = batches.size() - offset;
        const auto batch_size = std::min(config_.max_dispatch_batch, remaining);
        std::span<const IndexRetentionCandidate> span{batches.data() + offset, batch_size};

        IndexRetentionStats batch_stats{};
        auto dispatch_error = dispatch_(span, batch_stats);
        if (dispatch_error) {
            const auto dispatch_end = steady_now();
            const auto dispatch_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dispatch_end - dispatch_start);
            const auto dispatch_duration_ns = dispatch_ns.count() > 0 ? static_cast<std::uint64_t>(dispatch_ns.count()) : 0ULL;
            const auto run_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dispatch_end - run_start);
            const auto run_duration_ns = run_ns.count() > 0 ? static_cast<std::uint64_t>(run_ns.count()) : 0ULL;

            std::lock_guard guard{mutex_};
            telemetry_.dispatch_failures += 1U;
            telemetry_.checkpoint_failures += 1U;
            telemetry_.total_dispatch_duration_ns += dispatch_duration_ns;
            telemetry_.last_dispatch_duration_ns = dispatch_duration_ns;
            telemetry_.total_checkpoint_duration_ns += run_duration_ns;
            telemetry_.last_checkpoint_duration_ns = run_duration_ns;
            telemetry_.skipped_candidates += skipped_candidates;
            for (const auto& entry : selected) {
                pending_.insert_or_assign(entry.first, entry.second);
            }
            telemetry_.pending_candidates = pending_.size();
            return dispatch_error;
        }

        aggregate_stats.dispatched_candidates += span.size();
        aggregate_stats.pruned_candidates += batch_stats.pruned_candidates;
        aggregate_stats.skipped_candidates += batch_stats.skipped_candidates;
        ++batch_count;
        offset += batch_size;
    }

    const auto dispatch_end = steady_now();
    const auto dispatch_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dispatch_end - dispatch_start);
    const auto dispatch_duration_ns = dispatch_ns.count() > 0 ? static_cast<std::uint64_t>(dispatch_ns.count()) : 0ULL;

    const auto run_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dispatch_end - run_start);
    const auto run_duration_ns = run_ns.count() > 0 ? static_cast<std::uint64_t>(run_ns.count()) : 0ULL;

    if (stats) {
        stats->dispatched_candidates += aggregate_stats.dispatched_candidates;
        stats->pruned_candidates += aggregate_stats.pruned_candidates;
        stats->skipped_candidates += aggregate_stats.skipped_candidates;
    }

    std::lock_guard guard{mutex_};
    telemetry_.dispatch_batches += batch_count;
    telemetry_.dispatched_candidates += aggregate_stats.dispatched_candidates;
    telemetry_.pruned_candidates += aggregate_stats.pruned_candidates;
    telemetry_.skipped_candidates += skipped_candidates + aggregate_stats.skipped_candidates;
    telemetry_.total_dispatch_duration_ns += dispatch_duration_ns;
    telemetry_.last_dispatch_duration_ns = dispatch_duration_ns;
    telemetry_.total_checkpoint_duration_ns += run_duration_ns;
    telemetry_.last_checkpoint_duration_ns = run_duration_ns;
    telemetry_.pending_candidates = pending_.size();

    return {};
}

IndexRetentionManager::Config IndexRetentionManager::config() const noexcept
{
    return config_;
}

IndexRetentionTelemetrySnapshot IndexRetentionManager::telemetry_snapshot() const
{
    std::lock_guard guard{mutex_};
    auto snapshot = telemetry_;
    snapshot.pending_candidates = pending_.size();
    return snapshot;
}

}  // namespace bored::storage
