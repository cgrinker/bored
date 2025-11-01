#include "bored/storage/storage_runtime.hpp"

#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_telemetry_registry.hpp"
#include "bored/storage/temp_resource_registry.hpp"

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

    control_metrics_ = StorageControlTelemetrySnapshot{};

    control_telemetry_identifier_ = config_.control_telemetry_identifier;
    if (config_.storage_telemetry_registry && !control_telemetry_identifier_.empty()) {
        config_.storage_telemetry_registry->register_control(control_telemetry_identifier_, [this] {
            return this->control_telemetry_snapshot();
        });
    }

    install_control_handlers();

    return {};
}

void StorageRuntime::shutdown()
{
    uninstall_control_handlers();

    if (wal_writer_) {
        (void)wal_writer_->close();
        wal_writer_.reset();
    }

    checkpoint_scheduler_.reset();
    index_retention_manager_.reset();
    index_retention_pruner_.reset();
    index_retention_executor_.reset();
    checkpoint_manager_.reset();

    if (config_.storage_telemetry_registry && !control_telemetry_identifier_.empty()) {
        config_.storage_telemetry_registry->unregister_control(control_telemetry_identifier_);
        control_telemetry_identifier_.clear();
    }

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

    {
        std::lock_guard guard(control_metrics_mutex_);
        control_metrics_ = StorageControlTelemetrySnapshot{};
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

std::error_code StorageRuntime::run_control_command(ControlCommand command,
                                                    const std::function<std::error_code()>& callable)
{
    if (!callable) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto start = std::chrono::steady_clock::now();
    std::error_code result = callable();
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    const auto duration_ns = elapsed > 0 ? static_cast<std::uint64_t>(elapsed) : 0ULL;

    {
        std::lock_guard guard(control_metrics_mutex_);
        auto& metrics = control_metrics(command);
        metrics.attempts += 1U;
        if (result) {
            metrics.failures += 1U;
        }
        metrics.total_duration_ns += duration_ns;
        metrics.last_duration_ns = duration_ns;
    }

    return result;
}

OperationTelemetrySnapshot& StorageRuntime::control_metrics(ControlCommand command) noexcept
{
    switch (command) {
    case ControlCommand::Checkpoint:
        return control_metrics_.checkpoint;
    case ControlCommand::Retention:
        return control_metrics_.retention;
    case ControlCommand::Recovery:
        return control_metrics_.recovery;
    }
    return control_metrics_.checkpoint;
}

StorageControlTelemetrySnapshot StorageRuntime::control_telemetry_snapshot() const
{
    std::lock_guard guard(control_metrics_mutex_);
    return control_metrics_;
}

void StorageRuntime::install_control_handlers()
{
    if (control_handlers_installed_) {
        return;
    }

    previous_control_handlers_ = get_global_storage_control_handlers();

    StorageControlHandlers handlers{};
    handlers.checkpoint = [this](const StorageCheckpointRequest& request) {
        return this->handle_checkpoint_request(request);
    };
    handlers.retention = [this](const StorageRetentionRequest& request) {
        return this->handle_retention_request(request);
    };
    handlers.recovery = [this](const StorageRecoveryRequest& request) {
        return this->handle_recovery_request(request);
    };

    set_global_storage_control_handlers(std::move(handlers));
    control_handlers_installed_ = true;
}

void StorageRuntime::uninstall_control_handlers() noexcept
{
    if (!control_handlers_installed_) {
        return;
    }
    set_global_storage_control_handlers(previous_control_handlers_);
    control_handlers_installed_ = false;
}

std::error_code StorageRuntime::handle_checkpoint_request(const StorageCheckpointRequest& request)
{
    return run_control_command(ControlCommand::Checkpoint, [this, request]() -> std::error_code {
        if (!checkpoint_scheduler_) {
            return std::make_error_code(std::errc::not_connected);
        }

        const auto& provider = config_.checkpoint_snapshot_provider;
        if (!provider) {
            return std::make_error_code(std::errc::operation_not_permitted);
        }

        std::optional<WalAppendResult> append_result;
        const auto now = std::chrono::steady_clock::now();
        const std::optional<bool> dry_run_override = request.dry_run ? std::optional<bool>{true} : std::nullopt;
        return checkpoint_scheduler_->maybe_run(now, provider, request.force, append_result, dry_run_override);
    });
}

std::error_code StorageRuntime::handle_retention_request(const StorageRetentionRequest& request)
{
    return run_control_command(ControlCommand::Retention, [this, request]() -> std::error_code {
        if (!wal_writer_) {
            return std::make_error_code(std::errc::not_connected);
        }

        WalRetentionConfig retention_config = wal_writer_config_.retention;
        if (checkpoint_scheduler_) {
            retention_config = checkpoint_scheduler_->retention_config();
        }

        const auto segment_id = wal_writer_->current_segment_id();
        WalRetentionStats stats{};
        std::error_code result = wal_writer_->apply_retention(retention_config, segment_id, &stats);

        if (!result && request.include_index_retention && index_retention_manager_) {
            const auto now = std::chrono::steady_clock::now();
            const auto checkpoint_lsn = wal_writer_->next_lsn();
            auto index_ec = index_retention_manager_->apply_checkpoint(now, checkpoint_lsn, nullptr);
            if (index_ec) {
                result = index_ec;
            }
        }

        return result;
    });
}

std::error_code StorageRuntime::handle_recovery_request(const StorageRecoveryRequest& request)
{
    return run_control_command(ControlCommand::Recovery, [this, request]() -> std::error_code {
        WalRecoveryDriver driver = this->make_recovery_driver();
        WalRecoveryPlan plan{};
        if (auto plan_ec = driver.build_plan(plan); plan_ec) {
            return plan_ec;
        }

        WalReplayContext replay_context{};
        WalReplayer replayer{replay_context};

        if (request.cleanup_only) {
            WalRecoveryPlan cleanup_plan = plan;
            cleanup_plan.redo.clear();
            cleanup_plan.undo.clear();
            cleanup_plan.undo_spans.clear();
            return replayer.apply_undo(cleanup_plan);
        }

        if (request.run_redo) {
            if (auto redo_ec = replayer.apply_redo(plan); redo_ec) {
                return redo_ec;
            }
        }

        if (request.run_undo) {
            if (auto undo_ec = replayer.apply_undo(plan); undo_ec) {
                return undo_ec;
            }
        }

        return std::error_code{};
    });
}

}  // namespace bored::storage
