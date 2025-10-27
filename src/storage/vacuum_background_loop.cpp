#include "bored/storage/vacuum_background_loop.hpp"

#include <chrono>
#include <utility>

namespace bored::storage {

VacuumBackgroundLoop::VacuumBackgroundLoop(VacuumWorker::Config worker_config,
                                           VacuumWorker::SafeHorizonProvider safe_horizon,
                                           VacuumWorker::DispatchHook dispatch)
    : VacuumBackgroundLoop(std::move(worker_config),
                           std::move(safe_horizon),
                           std::move(dispatch),
                           Config{})
{
}

VacuumBackgroundLoop::VacuumBackgroundLoop(VacuumWorker::Config worker_config,
                                           VacuumWorker::SafeHorizonProvider safe_horizon,
                                           VacuumWorker::DispatchHook dispatch,
                                           Config loop_config)
    : config_{loop_config}
    , worker_{std::move(worker_config), std::move(safe_horizon), std::move(dispatch)}
{
}

VacuumBackgroundLoop::~VacuumBackgroundLoop()
{
    stop();
}

void VacuumBackgroundLoop::start()
{
    std::lock_guard lock(mutex_);
    if (running_) {
        return;
    }

    stop_requested_ = false;
    force_run_requested_ = config_.run_immediately;
    force_run_in_progress_ = false;
    running_ = true;
    thread_ = std::thread([this]() { run_loop(); });
}

void VacuumBackgroundLoop::stop()
{
    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return;
        }
        stop_requested_ = true;
        cv_.notify_all();
    }

    if (thread_.joinable()) {
        thread_.join();
    }

    std::lock_guard lock(mutex_);
    running_ = false;
    stop_requested_ = false;
    force_run_requested_ = false;
    force_run_in_progress_ = false;
}

void VacuumBackgroundLoop::request_force_run()
{
    std::lock_guard lock(mutex_);
    if (!running_) {
        force_run_requested_ = true;
        return;
    }
    if (force_run_requested_ || force_run_in_progress_) {
        return;
    }
    force_run_requested_ = true;
    cv_.notify_all();
}

void VacuumBackgroundLoop::schedule_page(std::uint32_t page_id, std::uint64_t prune_lsn)
{
    worker_.schedule_page(page_id, prune_lsn);
}

void VacuumBackgroundLoop::clear()
{
    worker_.clear();
}

bool VacuumBackgroundLoop::running() const noexcept
{
    std::lock_guard lock(mutex_);
    return running_;
}

std::optional<std::chrono::steady_clock::time_point> VacuumBackgroundLoop::last_run_time() const
{
    std::lock_guard lock(mutex_);
    return last_run_time_;
}

std::chrono::milliseconds VacuumBackgroundLoop::tick_interval() const noexcept
{
    return worker_.tick_interval();
}

VacuumTelemetrySnapshot VacuumBackgroundLoop::telemetry_snapshot() const
{
    return worker_.telemetry_snapshot();
}

std::optional<std::error_code> VacuumBackgroundLoop::last_error() const
{
    return worker_.last_error();
}

VacuumWorker& VacuumBackgroundLoop::worker() noexcept
{
    return worker_;
}

const VacuumWorker& VacuumBackgroundLoop::worker() const noexcept
{
    return worker_;
}

void VacuumBackgroundLoop::run_loop()
{
    const auto interval = worker_.tick_interval();
    std::unique_lock lock(mutex_);

    while (!stop_requested_) {
        bool force_run = false;

        if (force_run_requested_) {
            force_run = true;
            force_run_requested_ = false;
            force_run_in_progress_ = true;
        } else if (interval.count() == 0) {
            cv_.wait(lock, [this]() { return stop_requested_ || force_run_requested_; });
            continue;
        } else {
            const auto target_time = last_run_time_.has_value()
                                         ? (*last_run_time_ + interval)
                                         : (std::chrono::steady_clock::now() + interval);
            const bool should_wake = cv_.wait_until(lock, target_time, [this]() {
                return stop_requested_ || force_run_requested_;
            });

            if (should_wake) {
                if (stop_requested_) {
                    break;
                }

                if (force_run_requested_) {
                    force_run = true;
                    force_run_requested_ = false;
                    force_run_in_progress_ = true;
                } else {
                    continue;
                }
            }
        }

        lock.unlock();
        const auto now = std::chrono::steady_clock::now();
        const auto run_result = worker_.run_once(now, force_run);
        if (run_result) {
            // Worker tracks the error internally so the loop can rely on telemetry/last_error().
        }
        lock.lock();
        last_run_time_ = now;
        force_run_in_progress_ = false;
    }

    running_ = false;
    force_run_in_progress_ = false;
}

}  // namespace bored::storage
