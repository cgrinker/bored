#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/insert_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <filesystem>
#include <memory>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

using bored::executor::ExecutorContext;
using bored::executor::ExecutorContextConfig;
using bored::executor::ExecutorNode;
using bored::executor::InsertExecutor;
using bored::executor::TupleBuffer;
using bored::executor::TupleView;
using bored::executor::TupleWriter;
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

std::string payload_to_string(std::span<const std::byte> payload)
{
    return std::string(reinterpret_cast<const char*>(payload.data()), payload.size());
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
        auto span = std::span<const std::byte>(reinterpret_cast<const std::byte*>(value.data()), value.size());
        writer.append_column(span, false);
        writer.finalize();
        return true;
    }

    void close(ExecutorContext&) override {}

private:
    std::vector<std::string> values_{};
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
        auto ec = manager_->insert_tuple(page_span_, column.data, row_id, result, header);
        if (ec) {
            return ec;
        }

        results_.push_back(InsertOutcome{result, row_id});
        out_stats.payload_bytes = column.data.size();
        out_stats.wal_bytes = result.wal.written_bytes;
        return {};
    }

    std::error_code flush(ExecutorContext&) override
    {
        return manager_->flush_wal();
    }

    const std::vector<InsertOutcome>& outcomes() const noexcept { return results_; }

private:
    PageManager* manager_ = nullptr;
    std::span<std::byte> page_span_{};
    std::uint64_t next_row_id_ = 0U;
    std::vector<InsertOutcome> results_{};
};

}  // namespace

TEST_CASE("InsertExecutor writes tuples via PageManager")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_insert_executor_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(16) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    auto init_ec = manager.initialize_page(page_span, PageType::Table, 501U);
    REQUIRE_FALSE(init_ec);

    std::vector<std::string> rows{"alpha", "bravo", "charlie"};
    auto values_executor = std::make_unique<ValuesExecutor>(rows);

    bored::executor::ExecutorTelemetry telemetry;

    PageManagerInsertTarget target{&manager, page_span, 10'000U};

    InsertExecutor::Config config{};
    config.target = &target;
    config.telemetry = &telemetry;

    InsertExecutor executor{std::move(values_executor), config};

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(9001U, snapshot);

    executor.open(context);

    TupleBuffer sink_buffer{};
    REQUIRE_FALSE(executor.next(context, sink_buffer));

    executor.close(context);

    const auto& outcomes = target.outcomes();
    REQUIRE(outcomes.size() == rows.size());

    auto page_const = std::span<const std::byte>(page_buffer.data(), page_buffer.size());
    for (std::size_t index = 0; index < outcomes.size(); ++index) {
        const auto slot_index = outcomes[index].result.slot.index;
        auto storage = bored::storage::read_tuple_storage(page_const, slot_index);
        REQUIRE(storage.size() == tuple_storage_length(rows[index].size()));

        TupleHeader stored_header{};
        std::memcpy(&stored_header, storage.data(), tuple_header_size());
        REQUIRE(stored_header.created_transaction_id == context.transaction_id());
        REQUIRE(outcomes[index].row_id == 10'000U + index);

        auto payload = storage.subspan(tuple_header_size());
        REQUIRE(payload_to_string(payload) == rows[index]);
    }

    const auto snapshot_telemetry = telemetry.snapshot();
    REQUIRE(snapshot_telemetry.insert_rows_attempted == rows.size());
    REQUIRE(snapshot_telemetry.insert_rows_succeeded == rows.size());
    REQUIRE(snapshot_telemetry.insert_payload_bytes == (rows[0].size() + rows[1].size() + rows[2].size()));
    REQUIRE(snapshot_telemetry.insert_wal_bytes >= snapshot_telemetry.insert_payload_bytes);

    auto segment_path = wal_writer->segment_path(0U);
    REQUIRE(std::filesystem::exists(segment_path));

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}
