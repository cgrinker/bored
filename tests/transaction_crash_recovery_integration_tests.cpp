#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::FreeSpaceMap;
using bored::storage::PageHeader;
using bored::storage::PageManager;
using bored::storage::PageType;
using bored::storage::WalAppendResult;
using bored::storage::WalCommitHeader;
using bored::storage::WalRecoveryDriver;
using bored::storage::WalRecoveryPlan;
using bored::storage::WalRecoveredTransaction;
using bored::storage::WalRecoveredTransactionState;
using bored::storage::WalRecordDescriptor;
using bored::storage::WalRecordType;
using bored::storage::WalReplayContext;
using bored::storage::WalReplayer;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;
using bored::storage::TupleFlag;
using bored::storage::TupleHeader;
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
    auto base = std::filesystem::temp_directory_path();
    auto path = base / (prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    (void)std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

TupleHeader decode_tuple_header(std::span<const std::byte> storage)
{
    TupleHeader header{};
    if (storage.size() >= bored::storage::tuple_header_size()) {
        std::memcpy(&header, storage.data(), bored::storage::tuple_header_size());
    }
    return header;
}

WalCommitHeader make_commit_header(WalWriter& writer,
                                   std::uint64_t transaction_id,
                                   std::uint64_t next_transaction_id = 0U,
                                   std::uint64_t oldest_active_txn = 0U,
                                   std::uint64_t oldest_active_commit_lsn = 0U)
{
    WalCommitHeader header{};
    header.transaction_id = transaction_id;
    header.commit_lsn = writer.next_lsn();
    header.next_transaction_id = next_transaction_id != 0U ? next_transaction_id : (transaction_id + 1U);
    header.oldest_active_transaction_id = oldest_active_txn;
    header.oldest_active_commit_lsn = oldest_active_commit_lsn != 0U ? oldest_active_commit_lsn : header.commit_lsn;
    return header;
}

}  // namespace

TEST_CASE("Crash recovery preserves committed changes and rolls back aborting transactions", "[txn][integration]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_txn_crash_recovery_");

    WalWriterConfig writer_config{};
    writer_config.directory = wal_dir;
    writer_config.segment_size = 4U * bored::storage::kWalBlockSize;
    writer_config.buffer_size = 2U * bored::storage::kWalBlockSize;
    writer_config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, writer_config);
    FreeSpaceMap runtime_fsm;
    PageManager page_manager{&runtime_fsm, wal_writer};

    const std::uint32_t committed_page_id = 7'201U;
    const std::uint32_t aborting_page_id = 7'202U;

    alignas(8) std::array<std::byte, bored::storage::kPageSize> committed_page{};
    alignas(8) std::array<std::byte, bored::storage::kPageSize> aborting_page{};

    auto committed_span = std::span<std::byte>(committed_page.data(), committed_page.size());
    auto aborting_span = std::span<std::byte>(aborting_page.data(), aborting_page.size());

    REQUIRE_FALSE(page_manager.initialize_page(committed_span, PageType::Table, committed_page_id));
    REQUIRE_FALSE(page_manager.initialize_page(aborting_span, PageType::Table, aborting_page_id));

    const auto disk_committed_before = committed_page;

    const TransactionId committed_txn_id = 9'001U;
    const TransactionId aborting_txn_id = 9'002U;

    const std::array<std::byte, 6> committed_payload{
        std::byte{'c'}, std::byte{'o'}, std::byte{'m'}, std::byte{'m'}, std::byte{'i'}, std::byte{'t'}
    };
    TupleHeader committed_tuple_header{};
    committed_tuple_header.created_transaction_id = committed_txn_id;

    PageManager::TupleInsertResult committed_insert{};
    REQUIRE_FALSE(page_manager.insert_tuple(committed_span,
                                            std::span<const std::byte>(committed_payload.data(), committed_payload.size()),
                                            41U,
                                            committed_insert,
                                            committed_tuple_header));

    const std::array<std::byte, 6> aborting_payload{
        std::byte{'r'}, std::byte{'o'}, std::byte{'l'}, std::byte{'l'}, std::byte{'b'}, std::byte{'k'}
    };
    TupleHeader aborting_tuple_header{};
    aborting_tuple_header.created_transaction_id = aborting_txn_id;

    PageManager::TupleInsertResult aborting_insert{};
    REQUIRE_FALSE(page_manager.insert_tuple(aborting_span,
                                            std::span<const std::byte>(aborting_payload.data(), aborting_payload.size()),
                                            42U,
                                            aborting_insert,
                                            aborting_tuple_header));

    const auto disk_aborting_after_insert = aborting_page;

    auto commit_header = make_commit_header(*wal_writer, committed_page_id, committed_page_id + 1U, committed_page_id);
    WalAppendResult commit_append{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_append));

    WalRecordDescriptor abort_descriptor{};
    abort_descriptor.type = WalRecordType::Abort;
    abort_descriptor.page_id = aborting_page_id;
    abort_descriptor.flags = bored::storage::WalRecordFlag::None;
    abort_descriptor.payload = std::span<const std::byte>{};
    WalAppendResult abort_append{};
    REQUIRE_FALSE(wal_writer->append_record(abort_descriptor, abort_append));

    REQUIRE_FALSE(page_manager.close_wal());
    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver recovery_driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan recovery_plan{};
    REQUIRE_FALSE(recovery_driver.build_plan(recovery_plan));

    auto find_transaction = [&](std::uint64_t id) -> const WalRecoveredTransaction* {
        const auto it = std::find_if(recovery_plan.transactions.begin(), recovery_plan.transactions.end(), [&](const auto& txn) {
            return txn.transaction_id == id;
        });
        return it != recovery_plan.transactions.end() ? &(*it) : nullptr;
    };

    const auto* committed_tx = find_transaction(committed_page_id);
    REQUIRE(committed_tx != nullptr);
    CHECK(committed_tx->state == WalRecoveredTransactionState::Committed);
    CHECK(committed_tx->commit_record.has_value());

    const auto* aborting_tx = find_transaction(aborting_page_id);
    REQUIRE(aborting_tx != nullptr);
    CHECK(aborting_tx->state == WalRecoveredTransactionState::Aborted);
    CHECK_FALSE(aborting_tx->commit_record.has_value());

    REQUIRE(recovery_plan.redo.size() == 1U);
    REQUIRE(recovery_plan.undo.size() == 1U);
    REQUIRE(recovery_plan.undo_spans.size() == 1U);
    CHECK(recovery_plan.undo_spans.front().owner_page_id == aborting_page_id);

    FreeSpaceMap recovery_fsm;
    WalReplayContext replay_context(PageType::Table, &recovery_fsm);
    replay_context.set_page(committed_page_id, std::span<const std::byte>(disk_committed_before.data(), disk_committed_before.size()));
    replay_context.set_page(aborting_page_id, std::span<const std::byte>(disk_aborting_after_insert.data(), disk_aborting_after_insert.size()));

    WalReplayer replayer{replay_context};
    REQUIRE_FALSE(replayer.apply_redo(recovery_plan));

    {
        auto page = replay_context.get_page(committed_page_id);
        auto view = std::span<const std::byte>(page.data(), page.size());
        auto directory = bored::storage::slot_directory(view);
        REQUIRE(committed_insert.slot.index < directory.size());
        CHECK(directory[committed_insert.slot.index].length == committed_insert.slot.length);
        auto tuple_storage = bored::storage::read_tuple_storage(view, committed_insert.slot.index);
        REQUIRE_FALSE(tuple_storage.empty());
        auto tuple_header = decode_tuple_header(tuple_storage);
        CHECK(tuple_header.created_transaction_id == committed_txn_id);
        CHECK(tuple_header.deleted_transaction_id == 0U);
    }

    REQUIRE_FALSE(replayer.apply_undo(recovery_plan));

    {
        auto page = replay_context.get_page(aborting_page_id);
        auto view = std::span<const std::byte>(page.data(), page.size());
        auto directory = bored::storage::slot_directory(view);
        REQUIRE(aborting_insert.slot.index < directory.size());
        CHECK(directory[aborting_insert.slot.index].length == 0U);
        auto header = bored::storage::page_header(view);
        CHECK(header.fragment_count == 1U);
        auto tuple_storage = bored::storage::read_tuple_storage(view, aborting_insert.slot.index);
        CHECK(tuple_storage.empty());
    }

    (void)std::filesystem::remove_all(wal_dir);
}
