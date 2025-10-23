#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_undo_walker.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/free_space_map_persistence.hpp"
#include "bored/storage/async_io.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <limits>
#include <memory>
#include <vector>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::FreeSpaceMap;
using bored::storage::FreeSpaceMapPersistence;
using bored::storage::PageManager;
using bored::storage::PageType;
using bored::storage::WalRecoveryDriver;
using bored::storage::WalRecoveryPlan;
using bored::storage::WalRecoveryRecord;
using bored::storage::WalRecordDescriptor;
using bored::storage::WalRecordType;
using bored::storage::WalReplayContext;
using bored::storage::WalReplayer;
using bored::storage::WalUndoWalker;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;
using bored::storage::WalCompactionEntry;
using bored::storage::WalIndexMaintenanceAction;
using bored::storage::any;

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

}  // namespace

TEST_CASE("WalReplayer replays committed tuple changes")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_committed_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 4242U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    const auto disk_image_before = page_buffer;

    const std::array<std::byte, 5> tuple_insert{std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'}, std::byte{'o'}};
    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, tuple_insert, 1001U, insert_result));

    const std::array<std::byte, 7> tuple_update{std::byte{'u'}, std::byte{'p'}, std::byte{'d'}, std::byte{'a'}, std::byte{'t'}, std::byte{'e'}, std::byte{'!'}};
    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, tuple_update, 1001U, update_result));

    WalRecordDescriptor commit{};
    commit.type = WalRecordType::Commit;
    commit.page_id = page_id;
    commit.flags = bored::storage::WalRecordFlag::None;
    commit.payload = {};
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_record(commit, commit_result));

    const auto page_after = page_buffer;

    auto fsm_snapshot_path = wal_dir / "fsm.snapshot";
    REQUIRE_FALSE(FreeSpaceMapPersistence::write_snapshot(fsm, fsm_snapshot_path));

    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE_FALSE(plan.truncated_tail);
    REQUIRE(plan.redo.size() == 2U);

    {
        const auto& record = plan.redo.front();
        auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());
        auto meta = bored::storage::decode_wal_tuple_meta(payload);
        REQUIRE(meta);
    CAPTURE(meta->slot_index);
    CAPTURE(meta->tuple_length);
    }

    {
        const auto& record = plan.redo.back();
        auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());
        auto meta = bored::storage::decode_wal_tuple_update_meta(payload);
        REQUIRE(meta);
    CAPTURE(meta->base.slot_index);
    CAPTURE(meta->base.tuple_length);
    }

    FreeSpaceMap restored_fsm;
    REQUIRE_FALSE(FreeSpaceMapPersistence::load_snapshot(fsm_snapshot_path, restored_fsm));

    WalReplayContext context(PageType::Table, &restored_fsm);
    context.set_page(page_id, std::span<const std::byte>(disk_image_before.data(), disk_image_before.size()));

    WalReplayer replayer{context};

    WalRecoveryPlan insert_plan{};
    insert_plan.redo.push_back(plan.redo[0]);
    REQUIRE_FALSE(replayer.apply_redo(insert_plan));

    {
        auto page_after_insert = context.get_page(page_id);
        auto header = bored::storage::page_header(std::span<const std::byte>(page_after_insert.data(), page_after_insert.size()));
        CHECK(header.tuple_count == 1U);
        CHECK(header.fragment_count == 0U);
        CHECK(header.lsn == plan.redo[0].header.lsn);
    }

    WalRecoveryPlan update_plan{};
    update_plan.redo.push_back(plan.redo[1]);
    REQUIRE_FALSE(replayer.apply_redo(update_plan));

    auto repeat_ec = replayer.apply_redo(plan);
    if (repeat_ec) {
        FAIL("repeat_ec=" << repeat_ec.value() << " message=" << repeat_ec.message());
    }

    auto replayed_page = context.get_page(page_id);
    auto expected_page = std::span<const std::byte>(page_after.data(), page_after.size());

    REQUIRE(std::equal(replayed_page.begin(), replayed_page.end(), expected_page.begin(), expected_page.end()));

    CHECK(restored_fsm.current_free_bytes(page_id) == bored::storage::compute_free_bytes(bored::storage::page_header(expected_page)));
    CHECK(restored_fsm.current_fragment_count(page_id) == bored::storage::page_header(expected_page).fragment_count);

    std::filesystem::remove(fsm_snapshot_path);
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer replays overflow chunk records")
{
    FreeSpaceMap fsm;
    WalReplayContext context{PageType::Table, &fsm};
    WalReplayer replayer{context};

    bored::storage::WalOverflowChunkMeta chunk_meta{};
    chunk_meta.owner.page_id = 5123U;
    chunk_meta.owner.slot_index = 2U;
    chunk_meta.owner.tuple_length = 48U;
    chunk_meta.owner.row_id = 42U;
    chunk_meta.overflow_page_id = 9000U;
    chunk_meta.next_overflow_page_id = 0U;
    chunk_meta.chunk_offset = 0U;
    chunk_meta.chunk_length = 24U;
    chunk_meta.chunk_index = 0U;
    chunk_meta.flags = static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainStart |
                                                  bored::storage::WalOverflowChunkFlag::ChainEnd);

    std::array<std::byte, 24> chunk_payload{};
    for (std::size_t index = 0; index < chunk_payload.size(); ++index) {
        chunk_payload[index] = static_cast<std::byte>(index);
    }

    const auto payload_size = bored::storage::wal_overflow_chunk_payload_size(chunk_payload.size());
    std::vector<std::byte> wal_payload(payload_size);
    auto payload_span = std::span<std::byte>(wal_payload.data(), wal_payload.size());
    REQUIRE(bored::storage::encode_wal_overflow_chunk(payload_span,
                                                      chunk_meta,
                                                      std::span<const std::byte>(chunk_payload.data(), chunk_payload.size())));

    WalRecoveryRecord chunk_record{};
    chunk_record.header.type = static_cast<std::uint16_t>(WalRecordType::TupleOverflowChunk);
    chunk_record.header.lsn = 0xDEADBEEF;
    chunk_record.header.page_id = chunk_meta.overflow_page_id;
    chunk_record.header.total_length = static_cast<std::uint32_t>(sizeof(bored::storage::WalRecordHeader) + wal_payload.size());
    chunk_record.payload = wal_payload;

    WalRecoveryPlan redo_plan{};
    redo_plan.redo.push_back(chunk_record);
    REQUIRE_FALSE(replayer.apply_redo(redo_plan));

    auto page = context.get_page(chunk_meta.overflow_page_id);
    auto const_page = std::span<const std::byte>(page.data(), page.size());
    auto stored_meta = bored::storage::read_overflow_chunk_meta(const_page);
    REQUIRE(stored_meta);
    CHECK(stored_meta->owner.page_id == chunk_meta.owner.page_id);
    CHECK(stored_meta->chunk_length == chunk_meta.chunk_length);
    auto stored_data = bored::storage::overflow_chunk_payload(const_page, *stored_meta);
    REQUIRE(stored_data.size() == chunk_payload.size());
    REQUIRE(std::equal(stored_data.begin(), stored_data.end(), chunk_payload.begin(), chunk_payload.end()));
    CHECK(bored::storage::page_header(const_page).lsn == chunk_record.header.lsn);

    WalRecoveryPlan undo_plan{};
    undo_plan.undo.push_back(chunk_record);
    REQUIRE_FALSE(replayer.apply_undo(undo_plan));

    auto cleared_page = std::span<const std::byte>(page.data(), page.size());
    auto cleared_meta = bored::storage::read_overflow_chunk_meta(cleared_page);
    CHECK_FALSE(cleared_meta);
    CHECK(bored::storage::page_header(cleared_page).lsn == chunk_record.header.lsn);
}

TEST_CASE("WalReplayer truncates overflow chains")
{
    FreeSpaceMap fsm;
    WalReplayContext context{PageType::Table, &fsm};
    WalReplayer replayer{context};

    bored::storage::WalOverflowChunkMeta first_meta{};
    first_meta.owner.page_id = 7123U;
    first_meta.owner.slot_index = 5U;
    first_meta.owner.tuple_length = 64U;
    first_meta.owner.row_id = 84U;
    first_meta.overflow_page_id = 9100U;
    first_meta.next_overflow_page_id = 9101U;
    first_meta.chunk_offset = 0U;
    first_meta.chunk_length = 32U;
    first_meta.chunk_index = 0U;
    first_meta.flags = static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainStart);

    bored::storage::WalOverflowChunkMeta second_meta{};
    second_meta.owner = first_meta.owner;
    second_meta.overflow_page_id = 9101U;
    second_meta.next_overflow_page_id = 0U;
    second_meta.chunk_offset = first_meta.chunk_length;
    second_meta.chunk_length = 16U;
    second_meta.chunk_index = 1U;
    second_meta.flags = static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainEnd);

    std::array<std::byte, 32> first_payload{};
    for (std::size_t index = 0; index < first_payload.size(); ++index) {
        first_payload[index] = static_cast<std::byte>(0xA0 + index);
    }

    std::array<std::byte, 16> second_payload{};
    for (std::size_t index = 0; index < second_payload.size(); ++index) {
        second_payload[index] = static_cast<std::byte>(0xF0 + index);
    }

    auto make_chunk_record = [](const bored::storage::WalOverflowChunkMeta& meta,
                                std::span<const std::byte> data,
                                std::uint64_t lsn) {
        const auto payload_size = bored::storage::wal_overflow_chunk_payload_size(meta.chunk_length);
        std::vector<std::byte> wal_payload(payload_size);
        auto payload_span = std::span<std::byte>(wal_payload.data(), wal_payload.size());
        REQUIRE(bored::storage::encode_wal_overflow_chunk(payload_span, meta, data));

        WalRecoveryRecord record{};
        record.header.type = static_cast<std::uint16_t>(WalRecordType::TupleOverflowChunk);
        record.header.lsn = lsn;
        record.header.page_id = meta.overflow_page_id;
        record.header.total_length = static_cast<std::uint32_t>(sizeof(bored::storage::WalRecordHeader) + wal_payload.size());
        record.payload = std::move(wal_payload);
        return record;
    };

    auto first_chunk_record = make_chunk_record(first_meta, std::span<const std::byte>(first_payload.data(), first_payload.size()), 0x1000);
    auto second_chunk_record = make_chunk_record(second_meta, std::span<const std::byte>(second_payload.data(), second_payload.size()), 0x1100);

    WalRecoveryPlan chunk_plan{};
    chunk_plan.redo.push_back(first_chunk_record);
    chunk_plan.redo.push_back(second_chunk_record);
    REQUIRE_FALSE(replayer.apply_redo(chunk_plan));

    auto first_page = context.get_page(first_meta.overflow_page_id);
    auto second_page = context.get_page(second_meta.overflow_page_id);
    REQUIRE(bored::storage::read_overflow_chunk_meta(std::span<const std::byte>(first_page.data(), first_page.size())));
    REQUIRE(bored::storage::read_overflow_chunk_meta(std::span<const std::byte>(second_page.data(), second_page.size())));

    bored::storage::WalOverflowTruncateMeta truncate_meta{};
    truncate_meta.owner = first_meta.owner;
    truncate_meta.first_overflow_page_id = first_meta.overflow_page_id;
    truncate_meta.released_page_count = 2U;

    std::vector<bored::storage::WalOverflowChunkMeta> truncate_chunk_metas{first_meta, second_meta};
    std::vector<std::span<const std::byte>> truncate_payload_views{
        std::span<const std::byte>(first_payload.data(), first_payload.size()),
        std::span<const std::byte>(second_payload.data(), second_payload.size())};

    const auto truncate_payload_size = bored::storage::wal_overflow_truncate_payload_size(
        std::span<const bored::storage::WalOverflowChunkMeta>(truncate_chunk_metas.data(), truncate_chunk_metas.size()));
    std::vector<std::byte> truncate_payload(truncate_payload_size);
    REQUIRE(bored::storage::encode_wal_overflow_truncate(std::span<std::byte>(truncate_payload.data(), truncate_payload.size()),
                                                         truncate_meta,
                                                         std::span<const bored::storage::WalOverflowChunkMeta>(truncate_chunk_metas.data(), truncate_chunk_metas.size()),
                                                         std::span<const std::span<const std::byte>>(truncate_payload_views.data(), truncate_payload_views.size())));

    WalRecoveryRecord truncate_record{};
    truncate_record.header.type = static_cast<std::uint16_t>(WalRecordType::TupleOverflowTruncate);
    truncate_record.header.lsn = 0x1200;
    truncate_record.header.page_id = truncate_meta.first_overflow_page_id;
    truncate_record.header.total_length = static_cast<std::uint32_t>(sizeof(bored::storage::WalRecordHeader) + truncate_payload.size());
    truncate_record.payload = truncate_payload;

    WalRecoveryPlan truncate_plan{};
    truncate_plan.redo.push_back(truncate_record);
    REQUIRE_FALSE(replayer.apply_redo(truncate_plan));

    auto first_page_after = std::span<const std::byte>(first_page.data(), first_page.size());
    auto second_page_after = std::span<const std::byte>(second_page.data(), second_page.size());
    CHECK_FALSE(bored::storage::read_overflow_chunk_meta(first_page_after));
    CHECK_FALSE(bored::storage::read_overflow_chunk_meta(second_page_after));
    CHECK(bored::storage::page_header(first_page_after).lsn == truncate_record.header.lsn);
    CHECK(bored::storage::page_header(second_page_after).lsn == truncate_record.header.lsn);

    REQUIRE_FALSE(replayer.apply_redo(truncate_plan));
    auto first_page_reapplied = std::span<const std::byte>(first_page.data(), first_page.size());
    auto second_page_reapplied = std::span<const std::byte>(second_page.data(), second_page.size());
    CHECK_FALSE(bored::storage::read_overflow_chunk_meta(first_page_reapplied));
    CHECK_FALSE(bored::storage::read_overflow_chunk_meta(second_page_reapplied));
    CHECK(bored::storage::page_header(first_page_reapplied).lsn == truncate_record.header.lsn);
    CHECK(bored::storage::page_header(second_page_reapplied).lsn == truncate_record.header.lsn);

    WalRecoveryPlan undo_plan{};
    undo_plan.undo.push_back(truncate_record);
    REQUIRE_FALSE(replayer.apply_undo(undo_plan));

    auto first_page_restored = std::span<const std::byte>(first_page.data(), first_page.size());
    auto second_page_restored = std::span<const std::byte>(second_page.data(), second_page.size());
    auto restored_first_meta = bored::storage::read_overflow_chunk_meta(first_page_restored);
    auto restored_second_meta = bored::storage::read_overflow_chunk_meta(second_page_restored);
    REQUIRE(restored_first_meta);
    REQUIRE(restored_second_meta);
    CHECK(restored_first_meta->chunk_length == first_meta.chunk_length);
    CHECK(restored_second_meta->chunk_length == second_meta.chunk_length);
    auto restored_first_payload = bored::storage::overflow_chunk_payload(first_page_restored, *restored_first_meta);
    auto restored_second_payload = bored::storage::overflow_chunk_payload(second_page_restored, *restored_second_meta);
    REQUIRE(restored_first_payload.size() == first_payload.size());
    REQUIRE(restored_second_payload.size() == second_payload.size());
    REQUIRE(std::equal(restored_first_payload.begin(), restored_first_payload.end(), first_payload.begin(), first_payload.end()));
    REQUIRE(std::equal(restored_second_payload.begin(), restored_second_payload.end(), second_payload.begin(), second_payload.end()));
}

TEST_CASE("WalReplayer undoes uncommitted update using before image")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_undo_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 7777U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::array<std::byte, 24> insert_payload{};
    insert_payload.fill(std::byte{0x55});
    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, insert_payload, 9001U, insert_result));

    WalRecordDescriptor commit{};
    commit.type = WalRecordType::Commit;
    commit.page_id = page_id;
    commit.flags = bored::storage::WalRecordFlag::None;
    commit.payload = {};
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_record(commit, commit_result));

    const auto baseline_page = page_buffer;
    const auto baseline_free_bytes = fsm.current_free_bytes(page_id);

    std::array<std::byte, 32> updated_payload{};
    updated_payload.fill(std::byte{0xA7});
    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, updated_payload, 9001U, update_result));

    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.redo.size() == 1U);  // committed insert only
    REQUIRE(plan.undo.size() == 2U);
    REQUIRE(static_cast<WalRecordType>(plan.undo[0].header.type) == WalRecordType::TupleUpdate);
    REQUIRE(static_cast<WalRecordType>(plan.undo[1].header.type) == WalRecordType::TupleBeforeImage);

    FreeSpaceMap restored_fsm;
    WalReplayContext context{PageType::Table, &restored_fsm};
    context.set_page(page_id, std::span<const std::byte>(baseline_page.data(), baseline_page.size()));

    WalReplayer replayer{context};
    REQUIRE_FALSE(replayer.apply_redo(plan));
    auto undo_error = replayer.apply_undo(plan);
    CAPTURE(replayer.last_undo_type());
    REQUIRE_FALSE(undo_error);

    auto replayed_page = context.get_page(page_id);
    const auto replayed_header = bored::storage::page_header(
        std::span<const std::byte>(replayed_page.data(), replayed_page.size()));
    REQUIRE(replayed_header.page_id == page_id);
    REQUIRE(replayed_header.tuple_count == 1U);

    auto restored_tuple = bored::storage::read_tuple(std::span<const std::byte>(replayed_page.data(), replayed_page.size()), insert_result.slot.index);
    REQUIRE(restored_tuple.size() == insert_payload.size());
    REQUIRE(std::equal(restored_tuple.begin(), restored_tuple.end(), insert_payload.begin()));

    CHECK(restored_fsm.current_free_bytes(page_id) == baseline_free_bytes);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer undoes overflow delete using before-image chunks")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_overflow_before_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 40404U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<std::byte> payload(8192U);
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::byte>((index * 7U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, payload, 8080U, insert_result));
    REQUIRE(insert_result.used_overflow);

    WalRecordDescriptor commit{};
    commit.type = WalRecordType::Commit;
    commit.page_id = page_id;
    commit.flags = bored::storage::WalRecordFlag::None;
    commit.payload = {};
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_record(commit, commit_result));

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, insert_result.slot.index, 8080U, delete_result));

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    auto baseline_page = page_buffer;

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.undo_spans.size() == 1U);
    WalUndoWalker span_walker{plan};
    auto span_item = span_walker.next();
    REQUIRE(span_item);
    CHECK(span_item->owner_page_id == page_id);
    CHECK_FALSE(span_walker.next());

    auto before_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    REQUIRE(before_it != plan.undo.end());

    auto before_view = bored::storage::decode_wal_tuple_before_image(std::span<const std::byte>(before_it->payload.data(), before_it->payload.size()));
    REQUIRE(before_view);
    REQUIRE_FALSE(before_view->overflow_chunks.empty());
    REQUIRE(before_view->tuple_payload.size() == before_view->meta.tuple_length);
    CAPTURE(before_view->meta.slot_index);
    CAPTURE(before_view->meta.tuple_length);

    std::vector<bored::storage::WalOverflowChunkMeta> expected_chunk_metas;
    std::vector<std::vector<std::byte>> expected_chunk_payloads;
    expected_chunk_metas.reserve(before_view->overflow_chunks.size());
    expected_chunk_payloads.reserve(before_view->overflow_chunks.size());
    for (const auto& chunk_view : before_view->overflow_chunks) {
        expected_chunk_metas.push_back(chunk_view.meta);
        expected_chunk_payloads.emplace_back(chunk_view.payload.begin(), chunk_view.payload.end());
    }

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(baseline_page.data(), baseline_page.size()));
    WalReplayer replayer{context};

    std::vector<int> redo_types;
    redo_types.reserve(plan.redo.size());
    for (const auto& record : plan.redo) {
        redo_types.push_back(static_cast<int>(record.header.type));
    }
    CAPTURE(redo_types);

    auto delete_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleDelete;
    });
    if (delete_it != plan.undo.end()) {
        auto delete_meta = bored::storage::decode_wal_tuple_meta(std::span<const std::byte>(delete_it->payload.data(), delete_it->payload.size()));
        CAPTURE(delete_meta ? delete_meta->slot_index : static_cast<std::uint16_t>(std::numeric_limits<std::uint16_t>::max()));
    }

    std::vector<int> undo_types;
    undo_types.reserve(plan.undo.size());
    std::vector<std::uint32_t> undo_page_ids;
    undo_page_ids.reserve(plan.undo.size());
    std::vector<std::uint16_t> undo_slot_indices;
    undo_slot_indices.reserve(plan.undo.size());
    for (const auto& record : plan.undo) {
        undo_types.push_back(static_cast<int>(record.header.type));
        undo_page_ids.push_back(record.header.page_id);
        auto type = static_cast<WalRecordType>(record.header.type);
        if (type == WalRecordType::TupleInsert || type == WalRecordType::TupleDelete) {
            auto meta = bored::storage::decode_wal_tuple_meta(std::span<const std::byte>(record.payload.data(), record.payload.size()));
            undo_slot_indices.push_back(meta ? meta->slot_index : std::numeric_limits<std::uint16_t>::max());
        } else {
            undo_slot_indices.push_back(std::numeric_limits<std::uint16_t>::max());
        }
    }
    CAPTURE(undo_types);
    CAPTURE(undo_page_ids);
    CAPTURE(undo_slot_indices);

    REQUIRE_FALSE(replayer.apply_redo(plan));
    auto undo_error = replayer.apply_undo(plan);
    auto last_type = replayer.last_undo_type();
    CAPTURE(last_type.has_value());
    const int last_type_value = last_type ? static_cast<int>(*last_type) : -1;
    CAPTURE(last_type_value);
    REQUIRE_FALSE(undo_error);

    for (std::size_t index = 0; index < expected_chunk_metas.size(); ++index) {
        const auto& expected_meta = expected_chunk_metas[index];
        const auto& expected_payload = expected_chunk_payloads[index];
        auto page = context.get_page(expected_meta.overflow_page_id);
        auto restored_meta = bored::storage::read_overflow_chunk_meta(std::span<const std::byte>(page.data(), page.size()));
        REQUIRE(restored_meta);
        CHECK(restored_meta->overflow_page_id == expected_meta.overflow_page_id);
        CHECK(restored_meta->next_overflow_page_id == expected_meta.next_overflow_page_id);
        CHECK(restored_meta->chunk_index == expected_meta.chunk_index);
        CHECK(restored_meta->chunk_length == expected_meta.chunk_length);
        auto restored_payload = bored::storage::overflow_chunk_payload(std::span<const std::byte>(page.data(), page.size()), *restored_meta);
        REQUIRE(restored_payload.size() == expected_payload.size());
        REQUIRE(std::equal(restored_payload.begin(), restored_payload.end(), expected_payload.begin(), expected_payload.end()));
    }

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer undo overflow insert removes stub")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_overflow_insert_undo_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 50505U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<std::byte> payload(8192U);
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::byte>((index * 11U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, payload, 0xDEADULL, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE_FALSE(insert_result.overflow_page_ids.empty());

    const auto crash_snapshot = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    CAPTURE(plan.redo.size());
    CAPTURE(plan.undo.size());
    REQUIRE(plan.redo.empty());
    REQUIRE_FALSE(plan.undo.empty());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(crash_snapshot.data(), crash_snapshot.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto page_after = context.get_page(page_id);
    auto page_after_const = std::span<const std::byte>(page_after.data(), page_after.size());
    auto tuple_after = bored::storage::read_tuple(page_after_const, insert_result.slot.index);
    CAPTURE(tuple_after.size());
    CAPTURE(insert_result.inline_length);
    CAPTURE(insert_result.slot.index);

    REQUIRE(tuple_after.empty());

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer replays page compaction")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_compaction_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 6060U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::array<std::byte, 40> first_tuple{};
    first_tuple.fill(std::byte{0x33});
    PageManager::TupleInsertResult first_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, first_tuple, 301U, first_insert));

    std::array<std::byte, 56> second_tuple{};
    second_tuple.fill(std::byte{0x44});
    PageManager::TupleInsertResult second_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, second_tuple, 302U, second_insert));

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, first_insert.slot.index, 301U, delete_result));

    PageManager::PageCompactionResult compaction_result{};
    REQUIRE_FALSE(manager.compact_page(page_span, compaction_result));
    REQUIRE(compaction_result.performed);

    WalRecordDescriptor commit{};
    commit.type = WalRecordType::Commit;
    commit.page_id = page_id;
    commit.flags = bored::storage::WalRecordFlag::None;
    commit.payload = {};
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_record(commit, commit_result));

    const auto post_compaction_snapshot = page_buffer;

    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    std::vector<WalRecordType> redo_types;
    redo_types.reserve(plan.redo.size());
    bool compaction_found = false;
    for (const auto& record : plan.redo) {
        auto type = static_cast<WalRecordType>(record.header.type);
        redo_types.push_back(type);
        if (type == WalRecordType::PageCompaction) {
            compaction_found = true;
        }
    }
    CAPTURE(redo_types);
    REQUIRE(compaction_found);

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));

    auto replayed_page = context.get_page(page_id);
    auto expected_span = std::span<const std::byte>(post_compaction_snapshot.data(), post_compaction_snapshot.size());
    REQUIRE(std::equal(replayed_page.begin(), replayed_page.end(), expected_span.begin(), expected_span.end()));

    const auto& metadata = context.index_metadata();
    REQUIRE_FALSE(metadata.empty());
    bool refresh_seen = false;
    for (const auto& entry : metadata) {
        if (any(static_cast<WalIndexMaintenanceAction>(entry.index_action))) {
            refresh_seen = true;
        }
    }
    CHECK(refresh_seen);

    (void)std::filesystem::remove_all(wal_dir);
}
