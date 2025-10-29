#include "bored/storage/storage_runtime.hpp"

#include "bored/storage/wal_telemetry_registry.hpp"

#include <exception>
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

    try {
        checkpoint_scheduler_ = std::make_unique<CheckpointScheduler>(checkpoint_manager_, checkpoint_config);
    } catch (const std::system_error& error) {
        return error.code();
    } catch (const std::exception&) {
        return std::make_error_code(std::errc::invalid_argument);
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
    checkpoint_manager_.reset();

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

WalRecoveryDriver StorageRuntime::make_recovery_driver() const
{
    return WalRecoveryDriver{wal_writer_config_.directory,
                             wal_writer_config_.file_prefix,
                             wal_writer_config_.file_extension,
                             const_cast<TempResourceRegistry*>(&temp_registry())};
}

}  // namespace bored::storage
