#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_undo_walker.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/temp_resource_registry.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <vector>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::WalAppendResult;
using bored::storage::WalReader;
using bored::storage::WalRecordDescriptor;
using bored::storage::WalRecordType;
using bored::storage::WalRecoveryDriver;
using bored::storage::WalRecoveryPlan;
using bored::storage::WalRecoveredTransactionState;
using bored::storage::WalRecoveredTransaction;
using bored::storage::WalReplayContext;
using bored::storage::WalReplayer;
using bored::storage::FreeSpaceMap;
using bored::storage::PageManager;
using bored::storage::PageType;
using bored::storage::WalUndoWalker;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;
using bored::storage::TempResourceRegistry;

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
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

bored::storage::WalCommitHeader make_commit_header(WalWriter& writer,
                                                   std::uint64_t transaction_id,
                                                   std::uint64_t next_transaction_id = 0U,
                                                   std::uint64_t oldest_active_txn = 0U,
                                                   std::uint64_t oldest_active_commit_lsn = 0U)
{
    bored::storage::WalCommitHeader header{};
    header.transaction_id = transaction_id;
    header.commit_lsn = writer.next_lsn();
    header.next_transaction_id = next_transaction_id != 0U ? next_transaction_id : (transaction_id + 1U);
    header.oldest_active_transaction_id = oldest_active_txn;
    header.oldest_active_commit_lsn = oldest_active_commit_lsn != 0U ? oldest_active_commit_lsn : header.commit_lsn;
    return header;
}

}  // namespace

TEST_CASE("WalRecoveryDriver builds redo and undo plan")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_recovery_plan_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 2U * bored::storage::kWalBlockSize;
    config.buffer_size = bored::storage::kWalBlockSize;
    config.size_flush_threshold = bored::storage::kWalBlockSize;

    WalWriter writer{io, config};

    std::array<std::byte, 128> payload_a{};
    payload_a.fill(std::byte{0x11});

    std::array<std::byte, 96> payload_b{};
    payload_b.fill(std::byte{0x22});

    std::array<std::byte, 64> payload_c{};
    payload_c.fill(std::byte{0x33});

    WalRecordDescriptor descriptor{};

    descriptor.type = WalRecordType::TupleInsert;
    descriptor.page_id = 1001U;
    descriptor.payload = payload_a;
    WalAppendResult tx1_a{};
    REQUIRE_FALSE(writer.append_record(descriptor, tx1_a));

    descriptor.type = WalRecordType::TupleUpdate;
    descriptor.page_id = 1001U;
    descriptor.payload = payload_b;
    WalAppendResult tx1_b{};
    REQUIRE_FALSE(writer.append_record(descriptor, tx1_b));

    auto commit_header = make_commit_header(writer, 1001U, 1002U, 1001U);
    WalAppendResult tx1_commit{};
    REQUIRE_FALSE(writer.append_commit_record(commit_header, tx1_commit));

    descriptor.type = WalRecordType::TupleDelete;
    descriptor.page_id = 2002U;
    descriptor.payload = payload_c;
    WalAppendResult tx2_a{};
    REQUIRE_FALSE(writer.append_record(descriptor, tx2_a));

    descriptor.type = WalRecordType::Abort;
    descriptor.page_id = 2002U;
    descriptor.payload = {};
    WalAppendResult ignored_abort{};
    REQUIRE_FALSE(writer.append_record(descriptor, ignored_abort));

    REQUIRE_FALSE(writer.close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    auto ec = driver.build_plan(plan);
    REQUIRE_FALSE(ec);

    REQUIRE_FALSE(plan.truncated_tail);
    REQUIRE(plan.redo.size() == 2U);
    REQUIRE(plan.undo.size() == 1U);

    REQUIRE(plan.redo[0].header.lsn == tx1_a.lsn);
    REQUIRE(plan.redo[1].header.lsn == tx1_b.lsn);
    REQUIRE(plan.undo[0].header.lsn == tx2_a.lsn);

    REQUIRE(plan.transactions.size() == 2U);

    auto find_transaction = [&](std::uint64_t id) -> const bored::storage::WalRecoveredTransaction* {
        const auto it = std::find_if(plan.transactions.begin(), plan.transactions.end(), [&](const auto& txn) {
            return txn.transaction_id == id;
        });
        return it != plan.transactions.end() ? &(*it) : nullptr;
    };

    const auto* committed_tx = find_transaction(1001U);
    REQUIRE(committed_tx != nullptr);
    CHECK(committed_tx->state == WalRecoveredTransactionState::Committed);
    CHECK(committed_tx->commit_lsn == commit_header.commit_lsn);
    REQUIRE(committed_tx->commit_record.has_value());
    CHECK(committed_tx->commit_record->header.type == static_cast<std::uint16_t>(WalRecordType::Commit));
    CHECK(committed_tx->commit_record->header.lsn == tx1_commit.lsn);
    CHECK(committed_tx->first_lsn == tx1_a.lsn);
    CHECK(committed_tx->last_lsn == tx1_commit.lsn);

    const auto* aborted_tx = find_transaction(2002U);
    REQUIRE(aborted_tx != nullptr);
    CHECK(aborted_tx->state == WalRecoveredTransactionState::Aborted);
    CHECK_FALSE(aborted_tx->commit_record.has_value());
    CHECK(aborted_tx->commit_lsn == 0U);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalRecoveryDriver captures transaction allocator metadata")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_recovery_allocator_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 2U * bored::storage::kWalBlockSize;
    config.buffer_size = bored::storage::kWalBlockSize;
    config.size_flush_threshold = bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    WalWriter writer{io, config};

    const auto commit1_lsn = writer.next_lsn();
    auto commit_header_a = make_commit_header(writer, 101U, 501U, 123U, commit1_lsn - 64U);
    WalAppendResult commit_a{};
    REQUIRE_FALSE(writer.append_commit_record(commit_header_a, commit_a));

    const auto commit2_lsn = writer.next_lsn();
    auto commit_header_b = make_commit_header(writer, 202U, 450U, 99U, commit2_lsn - 128U);
    WalAppendResult commit_b{};
    REQUIRE_FALSE(writer.append_commit_record(commit_header_b, commit_b));

    auto commit_header_c = make_commit_header(writer, 303U);
    commit_header_c.next_transaction_id = 0U;
    commit_header_c.oldest_active_transaction_id = 0U;
    commit_header_c.oldest_active_commit_lsn = 0U;
    WalAppendResult commit_c{};
    REQUIRE_FALSE(writer.append_commit_record(commit_header_c, commit_c));

    REQUIRE_FALSE(writer.close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    CHECK(plan.next_transaction_id_high_water == 501U);
    CHECK(plan.oldest_active_transaction_id == 99U);
    const auto expected_oldest_commit_lsn = std::min(commit_header_a.oldest_active_commit_lsn,
                                                     commit_header_b.oldest_active_commit_lsn);
    CHECK(plan.oldest_active_commit_lsn == expected_oldest_commit_lsn);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalRecoveryDriver marks truncated tail")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_recovery_trunc_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 2U * bored::storage::kWalBlockSize;
    config.buffer_size = bored::storage::kWalBlockSize;
    config.size_flush_threshold = bored::storage::kWalBlockSize;

    WalWriter writer{io, config};

    std::array<std::byte, 80> payload_a{};
    payload_a.fill(std::byte{0x44});

    WalRecordDescriptor descriptor{};

    descriptor.type = WalRecordType::TupleInsert;
    descriptor.page_id = 501U;
    descriptor.payload = payload_a;
    WalAppendResult tx_commit{};
    REQUIRE_FALSE(writer.append_record(descriptor, tx_commit));

    auto commit_header = make_commit_header(writer, 501U, 502U, 501U);
    WalAppendResult tx_commit_record{};
    REQUIRE_FALSE(writer.append_commit_record(commit_header, tx_commit_record));

    descriptor.type = WalRecordType::TupleInsert;
    descriptor.page_id = 777U;
    descriptor.payload = payload_a;
    WalAppendResult truncated{};
    REQUIRE_FALSE(writer.append_record(descriptor, truncated));

    REQUIRE_FALSE(writer.close());
    io->shutdown();

    WalReader reader{wal_dir};
    std::vector<bored::storage::WalSegmentView> segments;
    REQUIRE_FALSE(reader.enumerate_segments(segments));
    const auto match = std::find_if(segments.begin(), segments.end(), [&](const auto& seg) {
        return seg.header.segment_id == truncated.segment_id;
    });
    REQUIRE(match != segments.end());

    auto segment_path = writer.segment_path(truncated.segment_id);
    const auto truncated_size = std::filesystem::file_size(segment_path);
    CAPTURE(truncated.segment_id);
    CAPTURE(truncated.lsn);
    CAPTURE(truncated.written_bytes);
    CAPTURE(truncated_size);
    const auto relative_offset = truncated.lsn - match->header.start_lsn;
    const auto new_size = bored::storage::kWalBlockSize + relative_offset + sizeof(bored::storage::WalRecordHeader) + payload_a.size() / 2U;
    REQUIRE(new_size < truncated_size);
    std::filesystem::resize_file(segment_path, new_size);

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    auto ec = driver.build_plan(plan);
    REQUIRE_FALSE(ec);

    REQUIRE(plan.truncated_tail);
    REQUIRE(plan.truncated_segment_id == truncated.segment_id);
    REQUIRE(plan.truncated_lsn == truncated.lsn);
    REQUIRE(plan.redo.size() == 1U);
    REQUIRE(plan.redo[0].header.lsn == tx_commit.lsn);
    REQUIRE(plan.undo.empty());

    REQUIRE(plan.transactions.size() == 1U);
    const auto& committed_tx = plan.transactions.front();
    CHECK(committed_tx.transaction_id == commit_header.transaction_id);
    CHECK(committed_tx.state == WalRecoveredTransactionState::Committed);
    CHECK(committed_tx.commit_lsn == commit_header.commit_lsn);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalRecoveryDriver flags in-flight transactions")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_recovery_inflight_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 2U * bored::storage::kWalBlockSize;
    config.buffer_size = bored::storage::kWalBlockSize;
    config.size_flush_threshold = bored::storage::kWalBlockSize;

    WalWriter writer{io, config};

    std::array<std::byte, 64> payload{};
    payload.fill(std::byte{0x55});

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::TupleInsert;
    descriptor.page_id = 31337U;
    descriptor.payload = payload;

    WalAppendResult append_result{};
    REQUIRE_FALSE(writer.append_record(descriptor, append_result));

    REQUIRE_FALSE(writer.close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.redo.empty());
    REQUIRE(plan.undo.size() == 1U);
    REQUIRE(plan.transactions.size() == 1U);

    const auto& txn = plan.transactions.front();
    CHECK(txn.transaction_id == descriptor.page_id);
    CHECK(txn.state == WalRecoveredTransactionState::InFlight);
    CHECK_FALSE(txn.commit_record.has_value());
    CHECK(txn.commit_lsn == 0U);
    CHECK(txn.first_lsn == append_result.lsn);
    CHECK(txn.last_lsn == append_result.lsn);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer purges executor temp resources after recovery")
{
    auto wal_dir = make_temp_dir("bored_wal_recovery_cleanup_");
    auto io = make_async_io();

    WalWriterConfig writer_config{};
    writer_config.directory = wal_dir;
    writer_config.segment_size = 4U * bored::storage::kWalBlockSize;
    writer_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    WalWriter writer{io, writer_config};

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::Abort;
    descriptor.page_id = 123U;

    WalAppendResult append{};
    REQUIRE_FALSE(writer.append_record(descriptor, append));
    REQUIRE_FALSE(writer.flush());
    REQUIRE_FALSE(writer.close());
    io->shutdown();

    auto spill_dir = wal_dir / "spill";
    std::filesystem::create_directories(spill_dir);
    auto spill_file = spill_dir / "temp_executor_file.tmp";
    {
        std::ofstream stream(spill_file, std::ios::binary);
        stream << "spill";
    }
    REQUIRE(std::filesystem::exists(spill_file));

    TempResourceRegistry registry;
    registry.register_directory(spill_dir);

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", &registry};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.temp_resource_registry == &registry);
    REQUIRE(std::filesystem::exists(spill_file));

    WalReplayContext replay_context{};
    WalReplayer replayer{replay_context};
    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    CHECK_FALSE(std::filesystem::exists(spill_file));

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalUndoWalker collates overflow undo records")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_undo_walker_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 13579U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<std::byte> payload(8192U);
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::byte>((index * 5U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, payload, 424242U, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE_FALSE(insert_result.overflow_page_ids.empty());

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, insert_result.slot.index, 424242U, delete_result));

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.redo.empty());
    REQUIRE(plan.undo_spans.size() == 1U);
    const auto& span = plan.undo_spans.front();
    CHECK(span.owner_page_id == page_id);
    CHECK(span.offset == 0U);
    CHECK(span.count == plan.undo.size());
    REQUIRE(span.count > 0U);

    WalUndoWalker walker{plan};
    auto item = walker.next();
    REQUIRE(item);
    CHECK(item->owner_page_id == page_id);
    CHECK(item->records.size() == plan.undo.size());

    for (auto overflow_id : insert_result.overflow_page_ids) {
        auto found = std::find(item->overflow_page_ids.begin(), item->overflow_page_ids.end(), overflow_id);
        REQUIRE(found != item->overflow_page_ids.end());
    }

    CHECK_FALSE(walker.next());

    (void)std::filesystem::remove_all(wal_dir);
}
