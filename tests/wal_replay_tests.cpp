#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/async_io.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <memory>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::FreeSpaceMap;
using bored::storage::PageManager;
using bored::storage::PageType;
using bored::storage::WalRecoveryDriver;
using bored::storage::WalRecoveryPlan;
using bored::storage::WalRecordDescriptor;
using bored::storage::WalRecordType;
using bored::storage::WalReplayContext;
using bored::storage::WalReplayer;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;

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

    WalReplayContext context;
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

    (void)std::filesystem::remove_all(wal_dir);
}
