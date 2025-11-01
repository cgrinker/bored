#pragma once

#include "bored/executor/executor_temp_resource_manager.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/checkpoint_scheduler.hpp"
#include "bored/storage/index_retention.hpp"
#include "bored/storage/index_retention_executor.hpp"
#include "bored/storage/index_retention_pruner.hpp"
#include "bored/storage/storage_control.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/wal_recovery.hpp"

#include <memory>
#include <optional>
#include <system_error>
#include <mutex>
#include <functional>

namespace bored::storage {

class WalTelemetryRegistry;

struct StorageRuntimeConfig final {
    AsyncIoConfig async_io{};
    std::shared_ptr<AsyncIo> async_io_instance{};
    WalTelemetryRegistry* wal_telemetry_registry = nullptr;
    StorageTelemetryRegistry* storage_telemetry_registry = nullptr;
    WalWriterConfig wal_writer{};
    CheckpointScheduler::Config checkpoint_scheduler{};
    CheckpointScheduler::SnapshotProvider checkpoint_snapshot_provider{};
    IndexRetentionManager::Config index_retention{};
    IndexRetentionManager::DispatchHook index_retention_dispatch{};
    std::optional<IndexRetentionPruner::Config> index_retention_pruner{};
    std::optional<IndexRetentionExecutor::Config> index_retention_executor{};
    bored::executor::ExecutorTempResourceManager::Config temp_manager{};
    std::string control_telemetry_identifier{};
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
    [[nodiscard]] IndexRetentionManager* index_retention_manager() const noexcept;

    [[nodiscard]] WalRecoveryDriver make_recovery_driver() const;

private:
    enum class ControlCommand {
        Checkpoint,
        Retention,
        Recovery
    };

    [[nodiscard]] std::error_code run_control_command(ControlCommand command,
                                                     const std::function<std::error_code()>& callable);

    [[nodiscard]] std::error_code handle_checkpoint_request(const StorageCheckpointRequest& request);
    [[nodiscard]] std::error_code handle_retention_request(const StorageRetentionRequest& request);
    [[nodiscard]] std::error_code handle_recovery_request(const StorageRecoveryRequest& request);

    [[nodiscard]] OperationTelemetrySnapshot& control_metrics(ControlCommand command) noexcept;
    [[nodiscard]] StorageControlTelemetrySnapshot control_telemetry_snapshot() const;
    void install_control_handlers();
    void uninstall_control_handlers() noexcept;

    StorageRuntimeConfig config_{};
    bored::executor::ExecutorTempResourceManager temp_manager_{};
    std::shared_ptr<AsyncIo> async_io_{};
    bool owns_async_io_ = false;
    WalWriterConfig wal_writer_config_{};
    std::shared_ptr<WalWriter> wal_writer_{};
    std::shared_ptr<CheckpointManager> checkpoint_manager_{};
    std::unique_ptr<CheckpointScheduler> checkpoint_scheduler_{};
    std::unique_ptr<IndexRetentionManager> index_retention_manager_{};
    std::unique_ptr<IndexRetentionPruner> index_retention_pruner_{};
    std::unique_ptr<IndexRetentionExecutor> index_retention_executor_{};
    StorageControlHandlers previous_control_handlers_{};
    bool control_handlers_installed_ = false;
    std::string control_telemetry_identifier_{};
    mutable std::mutex control_metrics_mutex_{};
    StorageControlTelemetrySnapshot control_metrics_{};
};

}  // namespace bored::storage
