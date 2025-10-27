#pragma once

#include "bored/storage/vacuum_worker.hpp"

#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <system_error>
#include <thread>

namespace bored::storage {

class VacuumBackgroundLoop final {
public:
    struct Config final {
        bool run_immediately = true;
    };

    VacuumBackgroundLoop(VacuumWorker::Config worker_config,
                         VacuumWorker::SafeHorizonProvider safe_horizon,
                         VacuumWorker::DispatchHook dispatch);
    VacuumBackgroundLoop(VacuumWorker::Config worker_config,
                         VacuumWorker::SafeHorizonProvider safe_horizon,
                         VacuumWorker::DispatchHook dispatch,
                         Config loop_config);
    ~VacuumBackgroundLoop();

    VacuumBackgroundLoop(const VacuumBackgroundLoop&) = delete;
    VacuumBackgroundLoop& operator=(const VacuumBackgroundLoop&) = delete;
    VacuumBackgroundLoop(VacuumBackgroundLoop&&) = delete;
    VacuumBackgroundLoop& operator=(VacuumBackgroundLoop&&) = delete;

    void start();
    void stop();
    void request_force_run();

    void schedule_page(std::uint32_t page_id, std::uint64_t prune_lsn);
    void clear();

    [[nodiscard]] bool running() const noexcept;
    [[nodiscard]] std::optional<std::chrono::steady_clock::time_point> last_run_time() const;

    [[nodiscard]] std::chrono::milliseconds tick_interval() const noexcept;
    [[nodiscard]] VacuumTelemetrySnapshot telemetry_snapshot() const;
    [[nodiscard]] std::optional<std::error_code> last_error() const;

    [[nodiscard]] VacuumWorker& worker() noexcept;
    [[nodiscard]] const VacuumWorker& worker() const noexcept;

private:
    void run_loop();

    Config config_{};
    VacuumWorker worker_;
    std::thread thread_{};
    mutable std::mutex mutex_{};
    std::condition_variable cv_{};
    bool running_ = false;
    bool stop_requested_ = false;
    bool force_run_requested_ = false;
    bool force_run_in_progress_ = false;
    std::optional<std::chrono::steady_clock::time_point> last_run_time_{};
};

}  // namespace bored::storage
