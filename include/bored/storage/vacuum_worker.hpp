#pragma once

#include "bored/storage/vacuum_scheduler.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <system_error>

namespace bored::storage {

class VacuumWorker final {
public:
    struct Config final {
        VacuumScheduler::Config scheduler{};
        std::chrono::milliseconds tick_interval{std::chrono::milliseconds{10}};
    };

    using SafeHorizonProvider = VacuumScheduler::SafeHorizonProvider;
    using DispatchHook = VacuumScheduler::DispatchHook;

    VacuumWorker(Config config,
                 SafeHorizonProvider safe_horizon,
                 DispatchHook dispatch) noexcept;
    ~VacuumWorker();

    VacuumWorker(const VacuumWorker&) = delete;
    VacuumWorker& operator=(const VacuumWorker&) = delete;
    VacuumWorker(VacuumWorker&&) = delete;
    VacuumWorker& operator=(VacuumWorker&&) = delete;

    void schedule_page(std::uint32_t page_id, std::uint64_t prune_lsn);
    void clear();

    [[nodiscard]] std::error_code run_once(std::chrono::steady_clock::time_point now, bool force = false);

    [[nodiscard]] std::chrono::milliseconds tick_interval() const noexcept;
    [[nodiscard]] VacuumTelemetrySnapshot telemetry_snapshot() const;
    [[nodiscard]] std::optional<std::error_code> last_error() const;

private:
    Config config_{};
    SafeHorizonProvider safe_horizon_{};
    DispatchHook dispatch_{};
    VacuumScheduler scheduler_{};
    mutable std::mutex error_mutex_{};
    std::optional<std::error_code> last_error_{};
};

}  // namespace bored::storage
