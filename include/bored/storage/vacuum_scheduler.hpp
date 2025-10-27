#pragma once

#include "bored/storage/storage_telemetry_registry.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace bored::storage {

struct VacuumWorkItem final {
    std::uint32_t page_id = 0U;
    std::uint64_t prune_lsn = 0U;
};

class VacuumScheduler final {
public:
    struct Config final {
        std::chrono::milliseconds min_interval{std::chrono::seconds(10)};
        std::size_t max_batch_pages = 128U;
        std::size_t max_pending_pages = 4096U;
        StorageTelemetryRegistry* telemetry_registry = nullptr;
        std::string telemetry_identifier{};
    };

    using SafeHorizonProvider = std::function<std::error_code(std::uint64_t&)>;
    using DispatchHook = std::function<std::error_code(std::span<const VacuumWorkItem>)>;

    VacuumScheduler();
    explicit VacuumScheduler(Config config);
    ~VacuumScheduler();

    VacuumScheduler(const VacuumScheduler&) = delete;
    VacuumScheduler& operator=(const VacuumScheduler&) = delete;
    VacuumScheduler(VacuumScheduler&&) = delete;
    VacuumScheduler& operator=(VacuumScheduler&&) = delete;

    void schedule_page(std::uint32_t page_id, std::uint64_t prune_lsn);
    void clear();

    [[nodiscard]] std::error_code maybe_run(std::chrono::steady_clock::time_point now,
                                            const SafeHorizonProvider& horizon_provider,
                                            const DispatchHook& dispatch,
                                            bool force = false);

    [[nodiscard]] std::size_t pending_pages() const noexcept;
    [[nodiscard]] Config config() const noexcept;
    [[nodiscard]] VacuumTelemetrySnapshot telemetry_snapshot() const;

private:
    void register_telemetry();
    void unregister_telemetry();
    void rebuild_lookup_locked();

    Config config_{};
    std::vector<VacuumWorkItem> pending_{};
    std::unordered_map<std::uint32_t, std::size_t> lookup_{};
    mutable std::mutex mutex_{};
    VacuumTelemetrySnapshot telemetry_{};
    std::chrono::steady_clock::time_point last_dispatch_time_{};
    bool has_last_dispatch_time_ = false;
};

}  // namespace bored::storage
