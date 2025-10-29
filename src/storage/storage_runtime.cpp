#include "bored/storage/storage_runtime.hpp"

#include "bored/storage/wal_telemetry_registry.hpp"

#include <chrono>
#include <exception>
#include <filesystem>
#include <span>
#include <utility>

namespace bored::storage {

StorageRuntime::StorageRuntime(StorageRuntimeConfig config)
    : config_{std::move(config)}
    , temp_manager_{config_.temp_manager}
{
    if (config_.async_io_instance) {
        async_io_ = config_.async_io_instance;
        owns_async_io_ = false;
    }
}

StorageRuntime::~StorageRuntime()
{
    shutdown();
}

std::error_code StorageRuntime::initialize()
{
    if (wal_writer_) {
        return std::make_error_code(std::errc::already_connected);
    }

    if (!async_io_) {
        auto io = create_async_io(config_.async_io);
        if (!io) {
            return std::make_error_code(std::errc::not_enough_memory);
        }
        async_io_ = std::shared_ptr<AsyncIo>(io.release());
        owns_async_io_ = true;
    }

    wal_writer_config_ = config_.wal_writer;
    wal_writer_config_.telemetry_registry = config_.wal_telemetry_registry;
    wal_writer_config_.storage_telemetry_registry = config_.storage_telemetry_registry;
    wal_writer_config_.temp_resource_registry = &temp_manager_.registry();

    try {
        wal_writer_ = std::make_shared<WalWriter>(async_io_, wal_writer_config_);
    } catch (const std::system_error& error) {
        return error.code();
    } catch (const std::exception&) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    checkpoint_manager_ = std::make_shared<CheckpointManager>(wal_writer_);

    auto checkpoint_config = config_.checkpoint_scheduler;
    checkpoint_config.telemetry_registry = config_.storage_telemetry_registry;
    if (!checkpoint_config.durability_horizon && wal_writer_config_.durability_horizon) {
        checkpoint_config.durability_horizon = wal_writer_config_.durability_horizon;
    }
    checkpoint_config.retention = wal_writer_config_.retention;

    auto index_retention_config = config_.index_retention;
    if (!index_retention_config.telemetry_registry) {
        index_retention_config.telemetry_registry = config_.storage_telemetry_registry;
    }
    if (index_retention_config.telemetry_registry && index_retention_config.telemetry_identifier.empty()) {
        index_retention_config.telemetry_identifier = "index_retention";
    }

    auto dispatch = config_.index_retention_dispatch;
    if (!dispatch) {
        IndexRetentionPruner::Config pruner_config{};
        if (config_.index_retention_pruner) {
            pruner_config = *config_.index_retention_pruner;
        }

        if (!pruner_config.prune_callback) {
            IndexRetentionExecutor::Config executor_config{};
            if (config_.index_retention_executor) {
                executor_config = *config_.index_retention_executor;
            }
            index_retention_executor_ = std::make_unique<IndexRetentionExecutor>(std::move(executor_config));
            pruner_config.prune_callback = [this](const IndexRetentionCandidate& candidate) -> std::error_code {
                if (!this->index_retention_executor_) {
                    return {};
                }
                return this->index_retention_executor_->prune(candidate);
            };
        } else {
            index_retention_executor_.reset();
        }

        index_retention_pruner_ = std::make_unique<IndexRetentionPruner>(std::move(pruner_config));
        dispatch = [this](std::span<const IndexRetentionCandidate> candidates, IndexRetentionStats& stats) -> std::error_code {
            if (!this->index_retention_pruner_) {
                return {};
            }
            return this->index_retention_pruner_->dispatch(candidates, stats);
        };
    } else {
        index_retention_pruner_.reset();
        index_retention_executor_.reset();
    }

    try {
        index_retention_manager_ = std::make_unique<IndexRetentionManager>(index_retention_config, std::move(dispatch));
    } catch (const std::system_error& error) {
        return error.code();
    } catch (const std::exception&) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    checkpoint_config.index_retention_hook = [this](std::chrono::steady_clock::time_point now,
                                                    std::uint64_t checkpoint_lsn,
                                                    IndexRetentionStats* stats) -> std::error_code {
        if (!this->index_retention_manager_) {
            return {};
        }
        return this->index_retention_manager_->apply_checkpoint(now, checkpoint_lsn, stats);
    };

    try {
        checkpoint_scheduler_ = std::make_unique<CheckpointScheduler>(checkpoint_manager_, checkpoint_config);
    } catch (const std::system_error& error) {
        return error.code();
    } catch (const std::exception&) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (config_.storage_telemetry_registry) {
        set_global_storage_telemetry_registry(config_.storage_telemetry_registry);
    }

    return {};
}

void StorageRuntime::shutdown()
{
    if (wal_writer_) {
        (void)wal_writer_->close();
        wal_writer_.reset();
    }

    checkpoint_scheduler_.reset();
    index_retention_manager_.reset();
    index_retention_pruner_.reset();
    index_retention_executor_.reset();
    checkpoint_manager_.reset();

    if (config_.storage_telemetry_registry &&
        get_global_storage_telemetry_registry() == config_.storage_telemetry_registry) {
        set_global_storage_telemetry_registry(nullptr);
    }

    if (async_io_ && owns_async_io_) {
        async_io_->shutdown();
        async_io_.reset();
        owns_async_io_ = false;
    } else {
        async_io_.reset();
    }
}

bored::executor::ExecutorTempResourceManager& StorageRuntime::temp_manager() noexcept
{
    return temp_manager_;
}

const bored::executor::ExecutorTempResourceManager& StorageRuntime::temp_manager() const noexcept
{
    return temp_manager_;
}

TempResourceRegistry& StorageRuntime::temp_registry() noexcept
{
    return temp_manager_.registry();
}

const TempResourceRegistry& StorageRuntime::temp_registry() const noexcept
{
    return temp_manager_.registry();
}

std::shared_ptr<AsyncIo> StorageRuntime::async_io() const noexcept
{
    return async_io_;
}

std::shared_ptr<WalWriter> StorageRuntime::wal_writer() const noexcept
{
    return wal_writer_;
}

std::shared_ptr<CheckpointManager> StorageRuntime::checkpoint_manager() const noexcept
{
    return checkpoint_manager_;
}

CheckpointScheduler* StorageRuntime::checkpoint_scheduler() const noexcept
{
    return checkpoint_scheduler_.get();
}

const WalWriterConfig& StorageRuntime::wal_writer_config() const noexcept
{
    return wal_writer_config_;
}

IndexRetentionManager* StorageRuntime::index_retention_manager() const noexcept
{
    return index_retention_manager_.get();
}

WalRecoveryDriver StorageRuntime::make_recovery_driver() const
{
    const auto checkpoint_directory = wal_writer_config_.directory / "checkpoints";
    return WalRecoveryDriver{wal_writer_config_.directory,
                             wal_writer_config_.file_prefix,
                             wal_writer_config_.file_extension,
                             const_cast<TempResourceRegistry*>(&temp_registry()),
                             checkpoint_directory,
                             config_.storage_telemetry_registry,
                             config_.storage_telemetry_registry ? std::string{"wal_recovery"} : std::string{}};
}

}  // namespace bored::storage
