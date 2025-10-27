#include "bored/storage/vacuum_scheduler.hpp"

#include <algorithm>

namespace bored::storage {

namespace {

std::uint64_t min_lsn(std::uint64_t lhs, std::uint64_t rhs)
{
    if (lhs == 0U) {
        return rhs;
    }
    if (rhs == 0U) {
        return lhs;
    }
    return std::min(lhs, rhs);
}

}  // namespace

VacuumScheduler::VacuumScheduler()
    : VacuumScheduler(Config{})
{
}

VacuumScheduler::VacuumScheduler(Config config)
    : config_{std::move(config)}
{
    if (config_.max_batch_pages == 0U) {
        config_.max_batch_pages = 1U;
    }
    if (config_.max_pending_pages == 0U) {
        config_.max_pending_pages = 1U;
    }
    register_telemetry();
}

VacuumScheduler::~VacuumScheduler()
{
    unregister_telemetry();
}

void VacuumScheduler::register_telemetry()
{
    if (config_.telemetry_registry == nullptr || config_.telemetry_identifier.empty()) {
        return;
    }
    config_.telemetry_registry->register_vacuum(config_.telemetry_identifier,
                                                [this]() { return telemetry_snapshot(); });
}

void VacuumScheduler::unregister_telemetry()
{
    if (config_.telemetry_registry == nullptr || config_.telemetry_identifier.empty()) {
        return;
    }
    config_.telemetry_registry->unregister_vacuum(config_.telemetry_identifier);
}

void VacuumScheduler::schedule_page(std::uint32_t page_id, std::uint64_t prune_lsn)
{
    if (page_id == 0U) {
        return;
    }

    std::lock_guard guard(mutex_);
    telemetry_.scheduled_pages += 1U;

    auto it = lookup_.find(page_id);
    if (it != lookup_.end()) {
        auto& entry = pending_[it->second];
        entry.prune_lsn = min_lsn(entry.prune_lsn, prune_lsn);
        return;
    }

    if (pending_.size() >= config_.max_pending_pages) {
        telemetry_.dropped_pages += 1U;
        return;
    }

    const std::size_t index = pending_.size();
    pending_.push_back(VacuumWorkItem{page_id, prune_lsn});
    lookup_.insert_or_assign(page_id, index);
    telemetry_.pending_pages = static_cast<std::uint64_t>(pending_.size());
}

void VacuumScheduler::clear()
{
    std::lock_guard guard(mutex_);
    pending_.clear();
    lookup_.clear();
    telemetry_.pending_pages = 0U;
}

std::error_code VacuumScheduler::maybe_run(std::chrono::steady_clock::time_point now,
                                           const SafeHorizonProvider& horizon_provider,
                                           const DispatchHook& dispatch,
                                           bool force)
{
    if (!horizon_provider) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    if (!dispatch) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    std::uint64_t safe_horizon = 0U;
    if (auto horizon_ec = horizon_provider(safe_horizon); horizon_ec) {
        return horizon_ec;
    }

    std::vector<VacuumWorkItem> ready;
    ready.reserve(config_.max_batch_pages);

    {
        std::lock_guard guard(mutex_);
        telemetry_.last_safe_horizon = safe_horizon;

        if (!force && has_last_dispatch_time_) {
            const auto elapsed = now - last_dispatch_time_;
            if (elapsed < config_.min_interval) {
                telemetry_.skipped_runs += 1U;
                return {};
            }
        }

        if (pending_.empty()) {
            telemetry_.skipped_runs += 1U;
            return {};
        }

        std::vector<VacuumWorkItem> remaining;
        remaining.reserve(pending_.size());

        const auto can_vacuum = [safe_horizon](const VacuumWorkItem& item) {
            if (item.prune_lsn == 0U) {
                return true;
            }
            if (safe_horizon == 0U) {
                return false;
            }
            return item.prune_lsn <= safe_horizon;
        };

        for (const auto& item : pending_) {
            if (ready.size() >= config_.max_batch_pages) {
                remaining.push_back(item);
                continue;
            }

            if (!can_vacuum(item)) {
                remaining.push_back(item);
                continue;
            }

            ready.push_back(item);
        }

        if (ready.empty()) {
            pending_ = std::move(remaining);
            rebuild_lookup_locked();
            telemetry_.pending_pages = static_cast<std::uint64_t>(pending_.size());
            telemetry_.skipped_runs += 1U;
            return {};
        }

        pending_ = std::move(remaining);
        rebuild_lookup_locked();
        telemetry_.pending_pages = static_cast<std::uint64_t>(pending_.size());
    }

    const auto dispatch_start = std::chrono::steady_clock::now();
    auto dispatch_ec = dispatch(std::span<const VacuumWorkItem>(ready.data(), ready.size()));
    const auto dispatch_end = std::chrono::steady_clock::now();
    const auto dispatch_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(dispatch_end - dispatch_start).count();

    std::lock_guard guard(mutex_);
    if (dispatch_ec) {
        // Requeue items so they can be attempted again on the next run.
        for (const auto& item : ready) {
            if (pending_.size() >= config_.max_pending_pages) {
                telemetry_.dropped_pages += 1U;
                continue;
            }
            const auto index = pending_.size();
            pending_.push_back(item);
            lookup_.insert_or_assign(item.page_id, index);
        }
        telemetry_.pending_pages = static_cast<std::uint64_t>(pending_.size());
        telemetry_.dispatch_failures += 1U;
        return dispatch_ec;
    }

    last_dispatch_time_ = now;
    has_last_dispatch_time_ = true;
    telemetry_.runs += 1U;
    if (force) {
        telemetry_.forced_runs += 1U;
    }
    telemetry_.batches_dispatched += 1U;
    telemetry_.pages_dispatched += static_cast<std::uint64_t>(ready.size());
    telemetry_.total_dispatch_duration_ns += static_cast<std::uint64_t>(dispatch_duration);
    telemetry_.last_dispatch_duration_ns = static_cast<std::uint64_t>(dispatch_duration);
    telemetry_.pending_pages = static_cast<std::uint64_t>(pending_.size());

    return {};
}

std::size_t VacuumScheduler::pending_pages() const noexcept
{
    std::lock_guard guard(mutex_);
    return pending_.size();
}

VacuumScheduler::Config VacuumScheduler::config() const noexcept
{
    return config_;
}

VacuumTelemetrySnapshot VacuumScheduler::telemetry_snapshot() const
{
    std::lock_guard guard(mutex_);
    return telemetry_;
}

void VacuumScheduler::rebuild_lookup_locked()
{
    lookup_.clear();
    lookup_.reserve(pending_.size());
    for (std::size_t index = 0; index < pending_.size(); ++index) {
        lookup_.insert_or_assign(pending_[index].page_id, index);
    }
}

}  // namespace bored::storage
