#include "bored/executor/delete_executor.hpp"
#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/insert_executor.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/executor/update_executor.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_reader.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/txn/wal_commit_pipeline.hpp"
#include "bored/txn/transaction_manager.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

using bored::executor::DeleteExecutor;
using bored::executor::ExecutorContext;
using bored::executor::ExecutorContextConfig;
using bored::executor::ExecutorNode;
using bored::executor::InsertExecutor;
using bored::executor::TupleBuffer;
using bored::executor::TupleView;
using bored::executor::TupleWriter;
using bored::executor::UpdateExecutor;
using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::FreeSpaceMap;
using bored::storage::PageManager;
using bored::storage::PageType;
using bored::storage::TupleHeader;
using bored::storage::WalReader;
using bored::storage::WalRecoveryDriver;
using bored::storage::WalRecoveryPlan;
using bored::storage::WalRecordType;
using bored::storage::WalReplayer;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;
using bored::txn::TransactionContext;
using bored::txn::TransactionId;
using bored::txn::TransactionIdAllocatorStub;
using bored::txn::WalCommitPipeline;
using bored::txn::TransactionManager;

namespace {

std::shared_ptr<AsyncIo> make_async_io()
{
    AsyncIoConfig config{};
    config.backend = AsyncIoBackend::ThreadPool;
    config.worker_threads = 2U;
    config.queue_depth = 16U;
    auto instance = bored::storage::create_async_io(config);
    return std::shared_ptr<AsyncIo>(std::move(instance));
}

std::filesystem::path make_temp_dir(const std::string& prefix)
{
    auto base = std::filesystem::temp_directory_path();
    auto path = base / (prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    (void)std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

class ValuesExecutor final : public ExecutorNode {
public:
    explicit ValuesExecutor(std::vector<std::string> values)
        : values_{std::move(values)}
    {}

    void open(ExecutorContext&) override { index_ = 0U; }

    bool next(ExecutorContext&, TupleBuffer& buffer) override
    {
        if (index_ >= values_.size()) {
            return false;
        }

        TupleWriter writer{buffer};
        writer.reset();
        const auto& value = values_[index_++];
        auto payload = std::span<const std::byte>(reinterpret_cast<const std::byte*>(value.data()), value.size());
        writer.append_column(payload, false);
        writer.finalize();
        return true;
    }

    void close(ExecutorContext&) override {}

private:
    std::vector<std::string> values_{};
    std::size_t index_ = 0U;
};

class UpdateInputExecutor final : public ExecutorNode {
public:
    struct Command final {
        std::uint64_t row_id = 0U;
        std::uint16_t slot_index = 0U;
        std::string payload{};
    };

    explicit UpdateInputExecutor(std::vector<Command> commands)
        : commands_{std::move(commands)}
    {}

    void open(ExecutorContext&) override { index_ = 0U; }

    bool next(ExecutorContext&, TupleBuffer& buffer) override
    {
        if (index_ >= commands_.size()) {
            return false;
        }

        TupleWriter writer{buffer};
        writer.reset();

        const auto& command = commands_[index_++];
        auto row_bytes = std::span<const std::byte>(reinterpret_cast<const std::byte*>(&command.row_id), sizeof(command.row_id));
        auto slot_bytes = std::span<const std::byte>(reinterpret_cast<const std::byte*>(&command.slot_index), sizeof(command.slot_index));
        auto payload_bytes = std::span<const std::byte>(reinterpret_cast<const std::byte*>(command.payload.data()), command.payload.size());

        writer.append_column(row_bytes, false);
        writer.append_column(slot_bytes, false);
        writer.append_column(payload_bytes, false);
        writer.finalize();
        return true;
    }

    void close(ExecutorContext&) override {}

private:
    std::vector<Command> commands_{};
    std::size_t index_ = 0U;
};

class DeleteInputExecutor final : public ExecutorNode {
public:
    struct Command final {
        std::uint64_t row_id = 0U;
        std::uint16_t slot_index = 0U;
    };

    explicit DeleteInputExecutor(std::vector<Command> commands)
        : commands_{std::move(commands)}
    {}

    void open(ExecutorContext&) override { index_ = 0U; }

    bool next(ExecutorContext&, TupleBuffer& buffer) override
    {
        if (index_ >= commands_.size()) {
            return false;
        }

        TupleWriter writer{buffer};
        writer.reset();

        const auto& command = commands_[index_++];
        auto row_bytes = std::span<const std::byte>(reinterpret_cast<const std::byte*>(&command.row_id), sizeof(command.row_id));
        auto slot_bytes = std::span<const std::byte>(reinterpret_cast<const std::byte*>(&command.slot_index), sizeof(command.slot_index));

        writer.append_column(row_bytes, false);
        writer.append_column(slot_bytes, false);
        writer.finalize();
        return true;
    }

    void close(ExecutorContext&) override {}

private:
    std::vector<Command> commands_{};
    std::size_t index_ = 0U;
};

class PageManagerInsertTarget final : public InsertExecutor::Target {
public:
    struct InsertOutcome final {
        PageManager::TupleInsertResult result{};
        std::uint64_t row_id = 0U;
    };

    PageManagerInsertTarget(PageManager* manager, std::span<std::byte> page_span, std::uint64_t starting_row_id)
        : manager_{manager}
        , page_span_{page_span}
        , next_row_id_{starting_row_id}
    {}

    std::error_code insert_tuple(const TupleView& tuple,
                                 ExecutorContext& context,
                                 InsertExecutor::InsertStats& out_stats) override
    {
        if (tuple.column_count() == 0U) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto column = tuple.column(0U);
        if (column.is_null) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        TupleHeader header{};
        header.created_transaction_id = context.transaction_id();

        PageManager::TupleInsertResult result{};
        const auto row_id = next_row_id_++;
        auto ec = manager_->insert_tuple(page_span_, column.data, row_id, result, header, context.transaction_context());
        if (ec) {
            return ec;
        }

        outcomes_.push_back(InsertOutcome{result, row_id});
        out_stats.payload_bytes = column.data.size();
        out_stats.wal_bytes = result.wal.written_bytes;
        return {};
    }

    std::error_code flush(ExecutorContext&) override
    {
        return manager_->flush_wal();
    }

    std::error_code register_transaction_hooks(TransactionContext& txn, ExecutorContext&) override
    {
        if (hooks_registered_) {
            return {};
        }
        hooks_registered_ = true;
        txn.on_commit([this]() {
            (void)manager_->flush_wal();
        });
        txn.on_abort([this]() {
            (void)manager_->flush_wal();
        });
        return manager_->flush_wal();
    }

    const std::vector<InsertOutcome>& outcomes() const noexcept { return outcomes_; }

private:
    PageManager* manager_ = nullptr;
    std::span<std::byte> page_span_{};
    std::uint64_t next_row_id_ = 0U;
    std::vector<InsertOutcome> outcomes_{};
    bool hooks_registered_ = false;
};

class PageManagerUpdateTarget final : public UpdateExecutor::Target {
public:
    struct UpdateOutcome final {
        PageManager::TupleUpdateResult result{};
        std::uint64_t row_id = 0U;
        std::string payload{};
    };

    PageManagerUpdateTarget(PageManager* manager, std::span<std::byte> page_span)
        : manager_{manager}
        , page_span_{page_span}
    {}

    std::error_code update_tuple(const TupleView& tuple,
                                 ExecutorContext& context,
                                 UpdateExecutor::UpdateStats& out_stats) override
    {
        if (tuple.column_count() != 3U) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto row_view = tuple.column(0U);
        const auto slot_view = tuple.column(1U);
        const auto payload_view = tuple.column(2U);
        if (row_view.is_null || slot_view.is_null || payload_view.is_null) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (row_view.data.size() != sizeof(std::uint64_t) || slot_view.data.size() != sizeof(std::uint16_t)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::uint64_t row_id = 0U;
        std::uint16_t slot_index = 0U;
        std::memcpy(&row_id, row_view.data.data(), sizeof(row_id));
        std::memcpy(&slot_index, slot_view.data.data(), sizeof(slot_index));

        const auto page_const = std::span<const std::byte>(page_span_.data(), page_span_.size());
        const auto existing_storage = bored::storage::read_tuple_storage(page_const, slot_index);
        if (existing_storage.size() <= bored::storage::tuple_header_size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        const auto existing_payload = existing_storage.subspan(bored::storage::tuple_header_size());

        PageManager::TupleUpdateResult result{};
        auto ec = manager_->update_tuple(page_span_,
                                         slot_index,
                                         payload_view.data,
                                         row_id,
                                         result,
                                         context.transaction_context());
        if (ec) {
            return ec;
        }

        out_stats.new_payload_bytes = payload_view.data.size();
        out_stats.old_payload_bytes = existing_payload.size();
        out_stats.wal_bytes = result.wal.written_bytes;

        outcomes_.push_back(UpdateOutcome{result, row_id, std::string(reinterpret_cast<const char*>(payload_view.data.data()), payload_view.data.size())});
        return {};
    }

    std::error_code flush(ExecutorContext&) override
    {
        return manager_->flush_wal();
    }

    std::error_code register_transaction_hooks(TransactionContext& txn, ExecutorContext&) override
    {
        if (hooks_registered_) {
            return {};
        }
        hooks_registered_ = true;
        txn.on_commit([this]() {
            (void)manager_->flush_wal();
        });
        txn.on_abort([this]() {
            (void)manager_->flush_wal();
        });
        return manager_->flush_wal();
    }

    const std::vector<UpdateOutcome>& outcomes() const noexcept { return outcomes_; }

private:
    PageManager* manager_ = nullptr;
    std::span<std::byte> page_span_{};
    std::vector<UpdateOutcome> outcomes_{};
    bool hooks_registered_ = false;
};

class PageManagerDeleteTarget final : public DeleteExecutor::Target {
public:
    struct DeleteOutcome final {
        PageManager::TupleDeleteResult result{};
        std::uint64_t row_id = 0U;
        std::uint16_t slot_index = 0U;
    };

    PageManagerDeleteTarget(PageManager* manager, std::span<std::byte> page_span)
        : manager_{manager}
        , page_span_{page_span}
    {}

    std::error_code delete_tuple(const TupleView& tuple,
                                 ExecutorContext& context,
                                 DeleteExecutor::DeleteStats& out_stats) override
    {
        (void)context;
        if (tuple.column_count() < 2U) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto row_view = tuple.column(0U);
        const auto slot_view = tuple.column(1U);
        if (row_view.is_null || slot_view.is_null) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (row_view.data.size() != sizeof(std::uint64_t) || slot_view.data.size() != sizeof(std::uint16_t)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::uint64_t row_id = 0U;
        std::uint16_t slot_index = 0U;
        std::memcpy(&row_id, row_view.data.data(), sizeof(row_id));
        std::memcpy(&slot_index, slot_view.data.data(), sizeof(slot_index));

        const auto page_const = std::span<const std::byte>(page_span_.data(), page_span_.size());
        const auto storage = bored::storage::read_tuple_storage(page_const, slot_index);
        std::size_t payload_bytes = 0U;
        if (storage.size() > bored::storage::tuple_header_size()) {
            payload_bytes = storage.size() - bored::storage::tuple_header_size();
        }

        PageManager::TupleDeleteResult result{};
        if (auto ec = manager_->delete_tuple(page_span_, slot_index, row_id, result, context.transaction_context()); ec) {
            return ec;
        }

        out_stats.reclaimed_bytes = payload_bytes;
        out_stats.wal_bytes = result.wal.written_bytes;

        outcomes_.push_back(DeleteOutcome{result, row_id, slot_index});
        return {};
    }

    std::error_code flush(ExecutorContext&) override
    {
        return manager_->flush_wal();
    }

    std::error_code register_transaction_hooks(TransactionContext& txn, ExecutorContext&) override
    {
        if (hooks_registered_) {
            return {};
        }
        hooks_registered_ = true;
        txn.on_commit([this]() {
            (void)manager_->flush_wal();
        });
        txn.on_abort([this]() {
            (void)manager_->flush_wal();
        });
        return manager_->flush_wal();
    }

    const std::vector<DeleteOutcome>& outcomes() const noexcept { return outcomes_; }

private:
    PageManager* manager_ = nullptr;
    std::span<std::byte> page_span_{};
    std::vector<DeleteOutcome> outcomes_{};
    bool hooks_registered_ = false;
};

ExecutorContext make_context(TransactionContext& txn)
{
    ExecutorContextConfig config{};
    config.transaction_id = txn.id();
    config.snapshot = txn.snapshot();
    config.transaction = &txn;
    return ExecutorContext{config};
}

}  // namespace

TEST_CASE("DML executors recover committed work and rollback in-flight deletes", "[executor][wal][recovery]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_executor_dml_recovery_");

    WalWriterConfig writer_config{};
    writer_config.directory = wal_dir;
    writer_config.segment_size = 4U * bored::storage::kWalBlockSize;
    writer_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, writer_config);
    FreeSpaceMap fsm;
    PageManager page_manager{&fsm, wal_writer};

    const std::uint32_t page_id = 77'001U;

    WalCommitPipeline pipeline{wal_writer};
    TransactionIdAllocatorStub allocator{page_id};
    TransactionManager txn_manager{allocator, &pipeline};
    alignas(16) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(page_manager.initialize_page(page_span, PageType::Table, page_id));

    // Transaction: staged insert + update
    auto dml_txn = txn_manager.begin();
    auto insert_context = make_context(dml_txn);

    auto values = std::make_unique<ValuesExecutor>(std::vector<std::string>{"alpha"});
    bored::executor::ExecutorTelemetry insert_telemetry;
    PageManagerInsertTarget insert_target{&page_manager, page_span, 50'000U};

    InsertExecutor::Config insert_config{};
    insert_config.target = &insert_target;
    insert_config.telemetry = &insert_telemetry;

    InsertExecutor insert_executor{std::move(values), insert_config};

    insert_executor.open(insert_context);
    TupleBuffer sink_buffer{};
    REQUIRE_FALSE(insert_executor.next(insert_context, sink_buffer));
    insert_executor.close(insert_context);

    REQUIRE(insert_target.outcomes().size() == 1U);
    const auto inserted_slot = insert_target.outcomes().front().result.slot.index;
    const auto inserted_row_id = insert_target.outcomes().front().row_id;

    auto update_context = make_context(dml_txn);

    UpdateInputExecutor::Command update_command{};
    update_command.row_id = inserted_row_id;
    update_command.slot_index = inserted_slot;
    update_command.payload = "bravo";

    auto update_input = std::make_unique<UpdateInputExecutor>(std::vector<UpdateInputExecutor::Command>{update_command});
    bored::executor::ExecutorTelemetry update_telemetry;
    PageManagerUpdateTarget update_target{&page_manager, page_span};

    UpdateExecutor::Config update_config{};
    update_config.target = &update_target;
    update_config.telemetry = &update_telemetry;

    UpdateExecutor update_executor{std::move(update_input), update_config};

    update_executor.open(update_context);
    REQUIRE_FALSE(update_executor.next(update_context, sink_buffer));
    update_executor.close(update_context);

    REQUIRE(update_target.outcomes().size() == 1U);

    txn_manager.commit(dml_txn);

    // Transaction 3: delete remains in-flight (simulated crash before commit)
    auto delete_txn = txn_manager.begin();
    auto delete_context = make_context(delete_txn);

    DeleteInputExecutor::Command delete_command{};
    delete_command.row_id = inserted_row_id;
    delete_command.slot_index = inserted_slot;

    auto delete_input = std::make_unique<DeleteInputExecutor>(std::vector<DeleteInputExecutor::Command>{delete_command});
    bored::executor::ExecutorTelemetry delete_telemetry;
    PageManagerDeleteTarget delete_target{&page_manager, page_span};

    DeleteExecutor::Config delete_config{};
    delete_config.target = &delete_target;
    delete_config.telemetry = &delete_telemetry;

    DeleteExecutor delete_executor{std::move(delete_input), delete_config};

    delete_executor.open(delete_context);
    REQUIRE_FALSE(delete_executor.next(delete_context, sink_buffer));
    delete_executor.close(delete_context);

    REQUIRE(delete_target.outcomes().size() == 1U);

    const auto crash_image = page_buffer;

    REQUIRE_FALSE(page_manager.close_wal());
    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    // Verify WAL contents via reader
    WalReader reader{wal_dir};
    std::vector<WalRecordType> observed_types;
    REQUIRE_FALSE(reader.for_each_record([&](const auto&, const bored::storage::WalRecordHeader& header, std::span<const std::byte>) {
        observed_types.push_back(static_cast<WalRecordType>(header.type));
        return true;
    }));

    auto count_type = [&](WalRecordType type) {
        return std::count(observed_types.begin(), observed_types.end(), type);
    };

    CHECK(count_type(WalRecordType::TupleInsert) >= 1);
    CHECK(count_type(WalRecordType::TupleUpdate) >= 1);
    CHECK(count_type(WalRecordType::TupleDelete) >= 1);
    CHECK(count_type(WalRecordType::Commit) == 1);

    // Build recovery plan
    WalRecoveryDriver recovery_driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(recovery_driver.build_plan(plan));

    auto find_txn = [&](TransactionId id) -> const bored::storage::WalRecoveredTransaction* {
        const auto it = std::find_if(plan.transactions.begin(), plan.transactions.end(), [&](const auto& txn) {
            return txn.transaction_id == id;
        });
        return it != plan.transactions.end() ? &(*it) : nullptr;
    };

    const auto* committed_entry = find_txn(dml_txn.id());
    REQUIRE(committed_entry != nullptr);
    CHECK(committed_entry->state == bored::storage::WalRecoveredTransactionState::Committed);

    std::string recovered_ids;
    for (const auto& candidate : plan.transactions) {
        if (!recovered_ids.empty()) {
            recovered_ids.append(", ");
        }
        recovered_ids.append(std::to_string(candidate.transaction_id));
        CAPTURE(candidate.transaction_id);
        CAPTURE(static_cast<int>(candidate.state));
    }
    CAPTURE(plan.transactions.size());
    INFO("Recovered transactions: [" << recovered_ids << "]");

    CAPTURE(plan.redo.size());
    REQUIRE(plan.undo.size() >= 1U);
    REQUIRE(plan.undo_spans.size() >= 1U);
    CHECK(plan.undo_spans.front().owner_page_id == page_id);

    const auto has_before_image = std::any_of(plan.undo.begin(), plan.undo.end(), [](const bored::storage::WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    CHECK(has_before_image);

    std::string undo_sequence;
    for (const auto& record : plan.undo) {
        if (!undo_sequence.empty()) {
            undo_sequence.append(", ");
        }
        undo_sequence.append(std::to_string(static_cast<int>(record.header.type)));
    }
    INFO("Undo record types: [" << undo_sequence << "]");

    // Apply recovery
    FreeSpaceMap recovery_fsm;
    bored::storage::WalReplayContext replay_context(PageType::Table, &recovery_fsm);
    replay_context.set_page(page_id, std::span<const std::byte>(crash_image.data(), crash_image.size()));

    WalReplayer replayer{replay_context};
    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    const auto recovered_page = replay_context.get_page(page_id);
    auto page_view = std::span<const std::byte>(recovered_page.data(), recovered_page.size());
    auto directory = bored::storage::slot_directory(page_view);
    REQUIRE(inserted_slot < directory.size());
    CHECK(directory[inserted_slot].length != 0U);

    auto tuple_storage = bored::storage::read_tuple_storage(page_view, inserted_slot);
    REQUIRE(tuple_storage.size() >= bored::storage::tuple_header_size());
    TupleHeader header{};
    std::memcpy(&header, tuple_storage.data(), bored::storage::tuple_header_size());
    CHECK(header.created_transaction_id == dml_txn.id());
    CHECK(header.deleted_transaction_id == 0U);

    auto payload = tuple_storage.subspan(bored::storage::tuple_header_size());
    std::string payload_text(reinterpret_cast<const char*>(payload.data()), payload.size());
    CHECK(payload_text == "bravo");

    (void)std::filesystem::remove_all(wal_dir);
}
