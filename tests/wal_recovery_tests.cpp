#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/async_io.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <memory>
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

    descriptor.type = WalRecordType::Commit;
    descriptor.page_id = 1001U;
    descriptor.payload = {};
    WalAppendResult tx1_commit{};
    REQUIRE_FALSE(writer.append_record(descriptor, tx1_commit));

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

    descriptor.type = WalRecordType::Commit;
    descriptor.page_id = 501U;
    descriptor.payload = {};
    WalAppendResult tx_commit_record{};
    REQUIRE_FALSE(writer.append_record(descriptor, tx_commit_record));

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

    (void)std::filesystem::remove_all(wal_dir);
}
