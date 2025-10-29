#include "bored/catalog/catalog_bootstrapper.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_mvcc.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_format.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_reader.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_writer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <vector>

namespace {

std::shared_ptr<bored::storage::AsyncIo> make_async_io()
{
    using namespace bored::storage;
    AsyncIoConfig config{};
    config.backend = AsyncIoBackend::ThreadPool;
    config.worker_threads = 2U;
    config.queue_depth = 16U;
    auto instance = create_async_io(config);
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

TEST_CASE("Catalog bootstrap seeds system relations")
{
    using namespace bored;
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_catalog_bootstrap_");

    storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 8U * storage::kWalBlockSize;
    wal_config.buffer_size = 4U * storage::kWalBlockSize;
    wal_config.start_lsn = storage::kWalBlockSize;

    auto wal_writer = std::make_shared<storage::WalWriter>(io, wal_config);
    storage::FreeSpaceMap fsm;
    storage::PageManager manager{&fsm, wal_writer};

    catalog::CatalogBootstrapper bootstrapper({&manager, &fsm, true});
    catalog::CatalogBootstrapArtifacts artifacts;
    auto ec = bootstrapper.run(artifacts);
    REQUIRE_FALSE(ec);

    const std::array<std::uint32_t, 5> expected_pages{
        catalog::kCatalogDatabasesPageId,
        catalog::kCatalogSchemasPageId,
        catalog::kCatalogTablesPageId,
        catalog::kCatalogColumnsPageId,
        catalog::kCatalogIndexesPageId
    };

    for (auto page_id : expected_pages) {
        auto it = artifacts.pages.find(page_id);
        REQUIRE(it != artifacts.pages.end());
        auto span = std::span<const std::byte>(it->second.data(), it->second.size());
        const auto& header = storage::page_header(span);
        REQUIRE(header.page_id == page_id);
        REQUIRE(static_cast<storage::PageType>(header.type) == storage::PageType::Meta);
        REQUIRE(header.tuple_count > 0U);
        if (page_id == catalog::kCatalogDatabasesPageId) {
            auto tuple = storage::read_tuple(span, 0U);
            REQUIRE(!tuple.empty());
            auto view = catalog::decode_catalog_database(tuple);
            REQUIRE(view);
            REQUIRE(view->database_id == catalog::kSystemDatabaseId);
            REQUIRE(view->name == "system");
            CHECK(catalog::has_flag(view->tuple.visibility_flags, catalog::CatalogVisibilityFlag::Frozen));
        } else if (page_id == catalog::kCatalogTablesPageId) {
            auto tuple = storage::read_tuple(span, 0U);
            REQUIRE(!tuple.empty());
            auto view = catalog::decode_catalog_table(tuple);
            REQUIRE(view);
            REQUIRE(view->relation_id == catalog::kCatalogDatabasesRelationId);
            REQUIRE(view->table_type == catalog::CatalogTableType::Catalog);
            CHECK(catalog::has_flag(view->tuple.visibility_flags, catalog::CatalogVisibilityFlag::Frozen));
        }
    }

    REQUIRE_FALSE(manager.flush_wal());

    storage::WalReader reader{wal_dir, wal_config.file_prefix, wal_config.file_extension};
    std::vector<storage::WalSegmentView> segments;
    REQUIRE_FALSE(reader.enumerate_segments(segments));
    REQUIRE_FALSE(segments.empty());

    std::vector<storage::WalRecordView> records;
    REQUIRE_FALSE(reader.read_records(segments.front(), records));

    bool saw_catalog_insert = false;
    for (const auto& record : records) {
        const auto type = static_cast<storage::WalRecordType>(record.header.type);
        if (type == storage::WalRecordType::CatalogInsert) {
            saw_catalog_insert = true;
        }
    }
    REQUIRE(saw_catalog_insert);

    auto close_ec = wal_writer->close();
    REQUIRE_FALSE(close_ec);
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Catalog bootstrap WAL replay rehydrates pages")
{
    using namespace bored;
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_catalog_replay_");

    storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 8U * storage::kWalBlockSize;
    wal_config.buffer_size = 4U * storage::kWalBlockSize;
    wal_config.start_lsn = storage::kWalBlockSize;

    auto wal_writer = std::make_shared<storage::WalWriter>(io, wal_config);
    storage::FreeSpaceMap fsm;
    storage::PageManager manager{&fsm, wal_writer};

    catalog::CatalogBootstrapper bootstrapper({&manager, &fsm, true});
    catalog::CatalogBootstrapArtifacts artifacts;
    auto ec = bootstrapper.run(artifacts);
    REQUIRE_FALSE(ec);

    storage::WalRecoveryDriver driver{wal_dir, wal_config.file_prefix, wal_config.file_extension};
    storage::WalRecoveryPlan plan;
    REQUIRE_FALSE(driver.build_plan(plan));
    INFO("redo_count=" << plan.redo.size());
    INFO("undo_count=" << plan.undo.size());

    storage::FreeSpaceMap replay_fsm;
    storage::WalReplayContext context{storage::PageType::Meta, &replay_fsm};
    storage::WalReplayer replayer{context};
    REQUIRE_FALSE(replayer.apply_redo(plan));

    const std::array<std::uint32_t, 5> expected_pages{
        catalog::kCatalogDatabasesPageId,
        catalog::kCatalogSchemasPageId,
        catalog::kCatalogTablesPageId,
        catalog::kCatalogColumnsPageId,
        catalog::kCatalogIndexesPageId
    };

    for (auto page_id : expected_pages) {
        auto replay_page_span = context.get_page(page_id);
        auto replay_view = std::span<const std::byte>(replay_page_span.data(), replay_page_span.size());
        const auto& replay_header = storage::page_header(replay_view);

        auto baseline = artifacts.pages.at(page_id);
        auto baseline_view = std::span<const std::byte>(baseline.data(), baseline.size());
        const auto& baseline_header = storage::page_header(baseline_view);

        REQUIRE(replay_header.page_id == baseline_header.page_id);
        REQUIRE(replay_header.tuple_count == baseline_header.tuple_count);

        for (std::uint16_t slot = 0U; slot < replay_header.tuple_count; ++slot) {
            auto replay_tuple = storage::read_tuple(replay_view, slot);
            auto baseline_tuple = storage::read_tuple(baseline_view, slot);
            REQUIRE(replay_tuple.size() == baseline_tuple.size());
            REQUIRE(std::equal(replay_tuple.begin(), replay_tuple.end(), baseline_tuple.begin(), baseline_tuple.end()));
        }
    }

    auto close_ec = wal_writer->close();
    REQUIRE_FALSE(close_ec);
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Catalog reserved identifiers do not collide")
{
    using namespace bored::catalog;

    const std::array<std::uint64_t, 29> id_values{
        kSystemDatabaseId.value,
        kSystemSchemaId.value,
        kCatalogDatabasesRelationId.value,
        kCatalogSchemasRelationId.value,
        kCatalogTablesRelationId.value,
        kCatalogColumnsRelationId.value,
        kCatalogIndexesRelationId.value,
        kCatalogDatabasesIdColumnId.value,
        kCatalogDatabasesNameColumnId.value,
        kCatalogSchemasIdColumnId.value,
        kCatalogSchemasDatabaseColumnId.value,
        kCatalogSchemasNameColumnId.value,
        kCatalogTablesIdColumnId.value,
        kCatalogTablesSchemaColumnId.value,
        kCatalogTablesTypeColumnId.value,
        kCatalogTablesPageColumnId.value,
        kCatalogTablesNameColumnId.value,
        kCatalogColumnsIdColumnId.value,
        kCatalogColumnsTableColumnId.value,
        kCatalogColumnsTypeColumnId.value,
        kCatalogColumnsOrdinalColumnId.value,
        kCatalogColumnsNameColumnId.value,
        kCatalogIndexesIdColumnId.value,
        kCatalogIndexesTableColumnId.value,
        kCatalogIndexesTypeColumnId.value,
        kCatalogIndexesRootPageColumnId.value,
        kCatalogIndexesComparatorColumnId.value,
        kCatalogIndexesFanoutColumnId.value,
        kCatalogIndexesNameColumnId.value
    };

    std::set<std::uint64_t> unique_ids(id_values.begin(), id_values.end());
    REQUIRE(unique_ids.size() == id_values.size());

    const std::array<std::uint64_t, 5> index_values{
        kCatalogDatabasesNameIndexId.value,
        kCatalogSchemasNameIndexId.value,
        kCatalogTablesNameIndexId.value,
        kCatalogColumnsNameIndexId.value,
        kCatalogIndexesNameIndexId.value
    };

    std::set<std::uint64_t> unique_index_ids(index_values.begin(), index_values.end());
    REQUIRE(unique_index_ids.size() == index_values.size());

    for (auto value : index_values) {
        REQUIRE(unique_ids.count(value) == 0U);
    }
}
