#include "bored/txn/wal_commit_pipeline.hpp"

#include "bored/storage/async_io.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_durability_horizon.hpp"
#include "bored/storage/wal_writer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <vector>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::WalAppendResult;
using bored::storage::WalCommitHeader;
using bored::storage::WalDurabilityHorizon;
using bored::storage::WalRecordHeader;
using bored::storage::WalRecordType;
using bored::storage::StorageTelemetryRegistry;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;

namespace {

std::shared_ptr<AsyncIo> make_async_io()
{
    AsyncIoConfig config{};
    config.backend = AsyncIoBackend::ThreadPool;
    config.worker_threads = 2U;
    config.queue_depth = 8U;
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

std::vector<std::byte> read_file_bytes(const std::filesystem::path& path)
{
    std::ifstream stream(path, std::ios::binary);
    REQUIRE(stream.good());
    stream.seekg(0, std::ios::end);
    const auto size = static_cast<std::size_t>(stream.tellg());
    stream.seekg(0, std::ios::beg);
    std::vector<std::byte> buffer(size);
    stream.read(reinterpret_cast<char*>(buffer.data()), static_cast<std::streamsize>(buffer.size()));
    REQUIRE(stream.good());
    return buffer;
}

}  // namespace

TEST_CASE("WalCommitPipeline flushes staged commit to WAL", "[txn][wal]")
{
    auto io = make_async_io();
    auto dir = make_temp_dir("bored_wal_commit_pipeline_flush_");

    WalWriterConfig config{};
    config.directory = dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.flush_on_commit = false;

    auto writer = std::make_shared<WalWriter>(io, config);

    bool durable_called = false;
    bored::txn::CommitTicket durable_ticket{};
    WalAppendResult durable_result{};

    bored::txn::WalCommitPipeline::Hooks hooks{};
    hooks.on_commit_durable = [&](const bored::txn::CommitTicket& ticket,
                                  const WalCommitHeader& header,
                                  const WalAppendResult& result) {
        durable_called = true;
        durable_ticket = ticket;
        durable_result = result;
        CHECK(header.commit_lsn == ticket.commit_sequence);
    };

    bored::txn::WalCommitPipeline pipeline{writer, hooks};

    bored::txn::CommitRequest request{};
    request.transaction_id = 42U;
    request.next_transaction_id = 43U;
    request.oldest_active_transaction_id = 24U;
    request.snapshot.read_lsn = 77U;
    request.oldest_snapshot_read_lsn = request.snapshot.read_lsn;

    const auto initial_lsn = writer->next_lsn();

    bored::txn::CommitTicket ticket{};
    auto prepare_ec = pipeline.prepare_commit(request, ticket);
    REQUIRE_FALSE(prepare_ec);
    CHECK(ticket.transaction_id == request.transaction_id);
    CHECK(writer->next_lsn() > initial_lsn);

    auto wal_path = writer->segment_path(0U);
    REQUIRE(std::filesystem::exists(wal_path));
    CHECK(std::filesystem::file_size(wal_path) == bored::storage::kWalBlockSize);

    auto flush_ec = pipeline.flush_commit(ticket);
    REQUIRE_FALSE(flush_ec);

    const auto size_after_flush = std::filesystem::file_size(wal_path);
    CHECK(size_after_flush > bored::storage::kWalBlockSize);

    pipeline.confirm_commit(ticket);
    CHECK(durable_called);
    CHECK(durable_ticket.transaction_id == ticket.transaction_id);
    CHECK(durable_ticket.commit_sequence == ticket.commit_sequence);
    CHECK(durable_result.segment_id == 0U);

    auto bytes = read_file_bytes(wal_path);
    REQUIRE(bytes.size() == size_after_flush);
    REQUIRE(bytes.size() >= bored::storage::kWalBlockSize + durable_result.written_bytes);

    const auto record_offset = bored::storage::kWalBlockSize;
    REQUIRE(bytes.size() >= record_offset + sizeof(WalRecordHeader) + bored::storage::wal_commit_payload_size());
    WalRecordHeader header{};
    std::memcpy(&header, bytes.data() + record_offset, sizeof(header));
    CHECK(static_cast<WalRecordType>(header.type) == WalRecordType::Commit);
    CHECK(header.lsn == ticket.commit_sequence);

    auto payload_view = std::span<const std::byte>(
        bytes.data() + record_offset + sizeof(WalRecordHeader),
        bored::storage::wal_commit_payload_size());

    auto decoded = bored::storage::decode_wal_commit(payload_view);
    REQUIRE(decoded.has_value());
    CHECK(decoded->transaction_id == request.transaction_id);
    CHECK(decoded->commit_lsn == ticket.commit_sequence);
    CHECK(decoded->next_transaction_id == request.next_transaction_id);
    CHECK(decoded->oldest_active_transaction_id == request.oldest_active_transaction_id);
    CHECK(decoded->oldest_active_commit_lsn == request.oldest_snapshot_read_lsn);

    auto close_ec = writer->close();
    CHECK_FALSE(close_ec);
}

TEST_CASE("WalCommitPipeline default hook advances durability horizon", "[txn][wal]")
{
    auto io = make_async_io();
    auto dir = make_temp_dir("bored_wal_commit_pipeline_horizon_");

    auto horizon = std::make_shared<WalDurabilityHorizon>();

    WalWriterConfig config{};
    config.directory = dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.flush_on_commit = false;
    config.durability_horizon = horizon;

    auto writer = std::make_shared<WalWriter>(io, config);
    bored::txn::WalCommitPipeline pipeline{writer};

    bored::txn::CommitRequest request{};
    request.transaction_id = 91U;
    request.next_transaction_id = 92U;
    request.oldest_active_transaction_id = 60U;
    request.snapshot.read_lsn = 1234U;
    request.oldest_snapshot_read_lsn = request.snapshot.read_lsn;

    bored::txn::CommitTicket ticket{};
    REQUIRE_FALSE(pipeline.prepare_commit(request, ticket));
    REQUIRE_FALSE(pipeline.flush_commit(ticket));
    pipeline.confirm_commit(ticket);

    CHECK(horizon->last_commit_lsn() == ticket.commit_sequence);
    CHECK(horizon->oldest_active_commit_lsn() == request.oldest_snapshot_read_lsn);
    CHECK(horizon->last_commit_segment_id() == 0U);

    REQUIRE_FALSE(writer->close());
}

TEST_CASE("WalCommitPipeline publishes durability telemetry", "[txn][wal]")
{
    auto io = make_async_io();
    auto dir = make_temp_dir("bored_wal_commit_pipeline_telemetry_");

    auto horizon = std::make_shared<WalDurabilityHorizon>();

    WalWriterConfig config{};
    config.directory = dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.flush_on_commit = false;
    config.durability_horizon = horizon;

    auto writer = std::make_shared<WalWriter>(io, config);
    StorageTelemetryRegistry registry;

    bored::txn::WalCommitPipeline pipeline{writer, {}, &registry, "wal_commit"};

    bored::txn::CommitRequest request{};
    request.transaction_id = 401U;
    request.next_transaction_id = 402U;
    request.oldest_active_transaction_id = 250U;
    request.snapshot.read_lsn = 2048U;
    request.oldest_snapshot_read_lsn = request.snapshot.read_lsn;

    bored::txn::CommitTicket ticket{};
    REQUIRE_FALSE(pipeline.prepare_commit(request, ticket));
    REQUIRE_FALSE(pipeline.flush_commit(ticket));
    pipeline.confirm_commit(ticket);

    const auto snapshot = registry.aggregate_durability_horizons();
    CHECK(snapshot.last_commit_lsn == ticket.commit_sequence);
    CHECK(snapshot.oldest_active_commit_lsn == request.oldest_snapshot_read_lsn);
    CHECK(snapshot.last_commit_segment_id == 0U);

    REQUIRE_FALSE(writer->close());
}

TEST_CASE("WalCommitPipeline rollback restores staged state", "[txn][wal]")
{
    auto io = make_async_io();
    auto dir = make_temp_dir("bored_wal_commit_pipeline_rollback_");

    WalWriterConfig config{};
    config.directory = dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.flush_on_commit = false;

    auto writer = std::make_shared<WalWriter>(io, config);
    bored::txn::WalCommitPipeline pipeline{writer};

    bored::txn::CommitRequest request{};
    request.transaction_id = 512U;
    request.next_transaction_id = 513U;
    request.oldest_active_transaction_id = 256U;
    request.snapshot.read_lsn = 2048U;
    request.oldest_snapshot_read_lsn = request.snapshot.read_lsn;

    const auto initial_lsn = writer->next_lsn();

    bored::txn::CommitTicket ticket{};
    auto prepare_ec = pipeline.prepare_commit(request, ticket);
    REQUIRE_FALSE(prepare_ec);
    CHECK(writer->next_lsn() > initial_lsn);

    auto wal_path = writer->segment_path(0U);
    REQUIRE(std::filesystem::exists(wal_path));
    CHECK(std::filesystem::file_size(wal_path) == bored::storage::kWalBlockSize);

    pipeline.rollback_commit(ticket);
    CHECK(writer->next_lsn() == initial_lsn);

    auto bytes = read_file_bytes(wal_path);
    CHECK(bytes.size() == bored::storage::kWalBlockSize);

    auto close_ec = writer->close();
    CHECK_FALSE(close_ec);
}
