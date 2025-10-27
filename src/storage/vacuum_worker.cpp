#include "bored/storage/vacuum_worker.hpp"

#include <utility>

namespace bored::storage {

VacuumWorker::VacuumWorker(Config config,
                           SafeHorizonProvider safe_horizon,
                           DispatchHook dispatch) noexcept
    : config_{std::move(config)}
    , safe_horizon_{std::move(safe_horizon)}
    , dispatch_{std::move(dispatch)}
    , scheduler_{config_.scheduler}
{
}

VacuumWorker::~VacuumWorker() = default;

void VacuumWorker::schedule_page(std::uint32_t page_id, std::uint64_t prune_lsn)
{
    scheduler_.schedule_page(page_id, prune_lsn);
}

void VacuumWorker::clear()
{
    scheduler_.clear();
}

std::error_code VacuumWorker::run_once(std::chrono::steady_clock::time_point now, bool force)
{
    if (!safe_horizon_ || !dispatch_) {
        std::lock_guard guard(error_mutex_);
        last_error_ = std::make_error_code(std::errc::invalid_argument);
        return *last_error_;
    }

    auto ec = scheduler_.maybe_run(now, safe_horizon_, dispatch_, force);

    std::lock_guard guard(error_mutex_);
    if (ec) {
        last_error_ = ec;
    } else {
        last_error_.reset();
    }
    return ec;
}

std::chrono::milliseconds VacuumWorker::tick_interval() const noexcept
{
    return config_.tick_interval;
}

VacuumTelemetrySnapshot VacuumWorker::telemetry_snapshot() const
{
    return scheduler_.telemetry_snapshot();
}

std::optional<std::error_code> VacuumWorker::last_error() const
{
    std::lock_guard guard(error_mutex_);
    return last_error_;
}

}  // namespace bored::storage
