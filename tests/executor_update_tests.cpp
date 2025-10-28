#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/executor/update_executor.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_writer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

using bored::executor::ExecutorContext;
using bored::executor::ExecutorContextConfig;
using bored::executor::ExecutorNode;
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
using bored::storage::tuple_header_size;
using bored::storage::tuple_storage_length;
using bored::txn::Snapshot;
using bored::txn::TransactionId;

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
    auto root = std::filesystem::temp_directory_path();
    auto dir = root / (prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    (void)std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

ExecutorContext make_context(TransactionId transaction_id, Snapshot snapshot)
{
    ExecutorContextConfig config{};
    config.transaction_id = transaction_id;
    config.snapshot = std::move(snapshot);
    return ExecutorContext{config};
}

class UpdateInputExecutor final : public ExecutorNode {
public:
    struct Command final {
        std::uint64_t row_id = 0U;
        std::uint16_t slot_index = 0U;
        std::string updated_value{};
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
        const auto row_id_bytes = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(&command.row_id), sizeof(command.row_id));
        const auto slot_bytes = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(&command.slot_index), sizeof(command.slot_index));
        const auto payload_bytes = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(command.updated_value.data()),
            command.updated_value.size());

        writer.append_column(row_id_bytes, false);
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

        const auto row_id_view = tuple.column(0U);
        const auto slot_view = tuple.column(1U);
        const auto payload_view = tuple.column(2U);
        if (row_id_view.is_null || slot_view.is_null || payload_view.is_null) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (row_id_view.data.size() != sizeof(std::uint64_t) ||
            slot_view.data.size() != sizeof(std::uint16_t)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::uint64_t row_id = 0U;
        std::uint16_t slot_index = 0U;
        std::memcpy(&row_id, row_id_view.data.data(), sizeof(row_id));
        std::memcpy(&slot_index, slot_view.data.data(), sizeof(slot_index));

        const auto page_const = std::span<const std::byte>(page_span_.data(), page_span_.size());
        const auto existing_storage = bored::storage::read_tuple_storage(page_const, slot_index);
        if (existing_storage.size() <= tuple_header_size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        const auto existing_payload = existing_storage.subspan(tuple_header_size());

        PageManager::TupleUpdateResult result{};
        auto ec = manager_->update_tuple(page_span_,
                                         slot_index,
                                         payload_view.data,
                                         row_id,
                                         result);
        if (ec) {
            return ec;
        }

        out_stats.new_payload_bytes = payload_view.data.size();
        out_stats.old_payload_bytes = existing_payload.size();
        out_stats.wal_bytes = result.wal.written_bytes;

        outcomes_.push_back(UpdateOutcome{result, row_id, std::string(reinterpret_cast<const char*>(payload_view.data.data()), payload_view.data.size())});
        (void)context;
        return {};
    }

    std::error_code flush(ExecutorContext&) override
    {
        return manager_->flush_wal();
    }

    const std::vector<UpdateOutcome>& outcomes() const noexcept { return outcomes_; }

private:
    PageManager* manager_ = nullptr;
    std::span<std::byte> page_span_{};
    std::vector<UpdateOutcome> outcomes_{};
};

}  // namespace

TEST_CASE("UpdateExecutor applies updates via PageManager")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_update_executor_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(16) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 777U));

    const std::vector<std::string> original{"alpha", "bravo", "charlie"};
    std::vector<PageManager::TupleInsertResult> inserts;
    std::vector<std::uint64_t> row_ids;
    inserts.reserve(original.size());
    row_ids.reserve(original.size());

    for (std::size_t index = 0; index < original.size(); ++index) {
        const auto row_id = 50'000U + index;
        TupleHeader header{};
        header.created_transaction_id = 42U;
        PageManager::TupleInsertResult result{};
        auto payload = std::span<const std::byte>(reinterpret_cast<const std::byte*>(original[index].data()), original[index].size());
        REQUIRE_FALSE(manager.insert_tuple(page_span, payload, row_id, result, header));
        inserts.push_back(result);
        row_ids.push_back(row_id);
    }

    std::vector<UpdateInputExecutor::Command> commands;
    const std::vector<std::string> updated{"alpha-updated", "bravo-updated", "charlie-updated"};
    for (std::size_t index = 0; index < inserts.size(); ++index) {
        UpdateInputExecutor::Command command{};
        command.row_id = row_ids[index];
        command.slot_index = inserts[index].slot.index;
        command.updated_value = updated[index];
        commands.push_back(std::move(command));
    }

    auto update_input = std::make_unique<UpdateInputExecutor>(std::move(commands));

    bored::executor::ExecutorTelemetry telemetry;
    PageManagerUpdateTarget target{&manager, page_span};

    UpdateExecutor::Config config{};
    config.target = &target;
    config.telemetry = &telemetry;

    UpdateExecutor executor{std::move(update_input), config};

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(9'001U, snapshot);

    executor.open(context);

    TupleBuffer sink_buffer{};
    REQUIRE_FALSE(executor.next(context, sink_buffer));

    executor.close(context);

    const auto& outcomes = target.outcomes();
    REQUIRE(outcomes.size() == updated.size());

    auto page_const = std::span<const std::byte>(page_buffer.data(), page_buffer.size());
    for (std::size_t index = 0; index < outcomes.size(); ++index) {
        const auto slot_index = outcomes[index].result.slot.index;
        auto storage = bored::storage::read_tuple_storage(page_const, slot_index);
        REQUIRE(storage.size() == tuple_storage_length(updated[index].size()));

        TupleHeader stored_header{};
        std::memcpy(&stored_header, storage.data(), tuple_header_size());
        REQUIRE(stored_header.created_transaction_id == 42U);
        REQUIRE(stored_header.deleted_transaction_id == 0U);

        auto payload = storage.subspan(tuple_header_size());
        const std::string payload_string(reinterpret_cast<const char*>(payload.data()), payload.size());
        REQUIRE(payload_string == updated[index]);
    }

    const auto snapshot_telemetry = telemetry.snapshot();
    REQUIRE(snapshot_telemetry.update_rows_attempted == updated.size());
    REQUIRE(snapshot_telemetry.update_rows_succeeded == updated.size());
    REQUIRE(snapshot_telemetry.update_new_payload_bytes ==
            (updated[0].size() + updated[1].size() + updated[2].size()));
    REQUIRE(snapshot_telemetry.update_old_payload_bytes ==
            (original[0].size() + original[1].size() + original[2].size()));
    REQUIRE(snapshot_telemetry.update_wal_bytes >= snapshot_telemetry.update_new_payload_bytes);

    auto segment_path = wal_writer->segment_path(0U);
    REQUIRE(std::filesystem::exists(segment_path));

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}
