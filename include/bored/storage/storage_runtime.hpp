#pragma once

#include "bored/executor/executor_temp_resource_manager.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/checkpoint_scheduler.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/wal_recovery.hpp"

#include <memory>
#include <system_error>

namespace bored::storage {

class WalTelemetryRegistry;

struct StorageRuntimeConfig final {
    AsyncIoConfig async_io{};
    std::shared_ptr<AsyncIo> async_io_instance{};
    WalTelemetryRegistry* wal_telemetry_registry = nullptr;
    StorageTelemetryRegistry* storage_telemetry_registry = nullptr;
    WalWriterConfig wal_writer{};
    CheckpointScheduler::Config checkpoint_scheduler{};
    bored::executor::ExecutorTempResourceManager::Config temp_manager{};
};

class StorageRuntime final {
public:
    explicit StorageRuntime(StorageRuntimeConfig config);
    ~StorageRuntime();

    StorageRuntime(const StorageRuntime&) = delete;
    StorageRuntime& operator=(const StorageRuntime&) = delete;
    StorageRuntime(StorageRuntime&&) = delete;
    StorageRuntime& operator=(StorageRuntime&&) = delete;

    [[nodiscard]] std::error_code initialize();
    void shutdown();

    [[nodiscard]] bored::executor::ExecutorTempResourceManager& temp_manager() noexcept;
    [[nodiscard]] const bored::executor::ExecutorTempResourceManager& temp_manager() const noexcept;
    [[nodiscard]] TempResourceRegistry& temp_registry() noexcept;
    [[nodiscard]] const TempResourceRegistry& temp_registry() const noexcept;

    [[nodiscard]] std::shared_ptr<AsyncIo> async_io() const noexcept;
    [[nodiscard]] std::shared_ptr<WalWriter> wal_writer() const noexcept;
    [[nodiscard]] std::shared_ptr<CheckpointManager> checkpoint_manager() const noexcept;
    [[nodiscard]] CheckpointScheduler* checkpoint_scheduler() const noexcept;
    [[nodiscard]] const WalWriterConfig& wal_writer_config() const noexcept;

    [[nodiscard]] WalRecoveryDriver make_recovery_driver() const;

private:
    StorageRuntimeConfig config_{};
    bored::executor::ExecutorTempResourceManager temp_manager_{};
    std::shared_ptr<AsyncIo> async_io_{};
    bool owns_async_io_ = false;
    WalWriterConfig wal_writer_config_{};
    std::shared_ptr<WalWriter> wal_writer_{};
    std::shared_ptr<CheckpointManager> checkpoint_manager_{};
    std::unique_ptr<CheckpointScheduler> checkpoint_scheduler_{};
};

}  // namespace bored::storage
