#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/ddl/ddl_handlers.hpp"
#include "bored/parser/ddl_command_builder.hpp"
#include "bored/parser/ddl_script_executor.hpp"
#include "bored/shell/shell_engine.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/txn/transaction_manager.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace bored;

namespace {

struct InMemoryCatalogStorage final {
    using Relation = std::map<std::uint64_t, std::vector<std::byte>>;

    void seed(catalog::RelationId relation_id, std::uint64_t row_id, std::vector<std::byte> payload)
    {
        relations_[relation_id.value][row_id] = std::move(payload);
    }

    void apply(const catalog::CatalogMutationBatch& batch)
    {
        for (const auto& mutation : batch.mutations) {
            auto& relation = relations_[mutation.relation_id.value];
            switch (mutation.kind) {
            case catalog::CatalogMutationKind::Insert:
            case catalog::CatalogMutationKind::Update:
                if (mutation.after) {
                    relation[mutation.row_id] = mutation.after->payload;
                }
                break;
            case catalog::CatalogMutationKind::Delete:
                relation.erase(mutation.row_id);
                break;
            }
        }
    }

    [[nodiscard]] catalog::CatalogAccessor::RelationScanner make_scanner() const
    {
        return [this](catalog::RelationId relation_id, const catalog::CatalogAccessor::TupleCallback& callback) {
            auto it = relations_.find(relation_id.value);
            if (it == relations_.end()) {
                return;
            }
            for (const auto& [row_id, payload] : it->second) {
                (void)row_id;
                callback(std::span<const std::byte>(payload.data(), payload.size()));
            }
        };
    }

    [[nodiscard]] std::vector<catalog::CatalogTableView> list_tables() const
    {
        std::vector<catalog::CatalogTableView> result;
        auto it = relations_.find(catalog::kCatalogTablesRelationId.value);
        if (it == relations_.end()) {
            return result;
        }
        for (const auto& [row_id, payload] : it->second) {
            (void)row_id;
            auto decoded = catalog::decode_catalog_table(std::span<const std::byte>(payload.data(), payload.size()));
            if (decoded) {
                result.push_back(*decoded);
            }
        }
        return result;
    }

    [[nodiscard]] std::vector<catalog::CatalogColumnView> list_columns() const
    {
        std::vector<catalog::CatalogColumnView> result;
        auto it = relations_.find(catalog::kCatalogColumnsRelationId.value);
        if (it == relations_.end()) {
            return result;
        }
        for (const auto& [row_id, payload] : it->second) {
            (void)row_id;
            auto decoded = catalog::decode_catalog_column(std::span<const std::byte>(payload.data(), payload.size()));
            if (decoded) {
                result.push_back(*decoded);
            }
        }
        return result;
    }

    std::unordered_map<std::uint64_t, Relation> relations_{};
};

[[nodiscard]] txn::Snapshot make_snapshot() noexcept
{
    txn::Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = std::numeric_limits<std::uint64_t>::max();
    return snapshot;
}

struct IdentifierAllocator final : catalog::CatalogIdentifierAllocator {
    catalog::SchemaId allocate_schema_id() override
    {
        return catalog::SchemaId{++schema_ids_};
    }

    catalog::RelationId allocate_table_id() override
    {
        return catalog::RelationId{++table_ids_};
    }

    catalog::IndexId allocate_index_id() override
    {
        return catalog::IndexId{++index_ids_};
    }

    catalog::ColumnId allocate_column_id() override
    {
        return catalog::ColumnId{++column_ids_};
    }

    std::uint64_t schema_ids_ = 1'000U;
    std::uint64_t table_ids_ = 2'000U;
    std::uint64_t index_ids_ = 3'000U;
    std::uint64_t column_ids_ = 4'000U;
};

void seed_schema(InMemoryCatalogStorage& storage, catalog::SchemaId schema_id, catalog::DatabaseId database_id, std::string_view name)
{
    catalog::CatalogSchemaDescriptor descriptor{};
    descriptor.tuple.xmin = 1U;
    descriptor.tuple.xmax = 0U;
    descriptor.schema_id = schema_id;
    descriptor.database_id = database_id;
    descriptor.name = name;
    storage.seed(catalog::kCatalogSchemasRelationId, schema_id.value, catalog::serialize_catalog_schema(descriptor));
}

struct ShellHarness final {
    ShellHarness()
        : snapshot_manager_{make_snapshot()}
        , txn_manager_{txn_allocator_}
    {
        catalog::CatalogCache::instance().reset();

        seed_schema(storage_, default_schema_id_, catalog::kSystemDatabaseId, "analytics");

        ddl::DdlCommandDispatcher::Config dispatcher_cfg{};
        dispatcher_cfg.transaction_factory = [this](txn::TransactionContext* context) {
            catalog::CatalogTransactionConfig tx_cfg{&txn_allocator_, &snapshot_manager_};
            auto transaction = std::make_unique<catalog::CatalogTransaction>(tx_cfg);
            transaction->bind_transaction_context(&txn_manager_, context);
            return transaction;
        };
        dispatcher_cfg.mutator_factory = [this](catalog::CatalogTransaction& transaction) {
            catalog::CatalogMutatorConfig mutator_cfg{};
            mutator_cfg.transaction = &transaction;
            mutator_cfg.commit_lsn_provider = [] { return 0ULL; };
            auto mutator = std::make_unique<catalog::CatalogMutator>(mutator_cfg);
            auto* raw = mutator.get();
            transaction.register_commit_hook([this, raw]() -> std::error_code {
                if (!raw->has_published_batch()) {
                    return {};
                }
                auto batch = raw->consume_published_batch();
                storage_.apply(batch);
                return {};
            });
            return mutator;
        };
        dispatcher_cfg.accessor_factory = [this](catalog::CatalogTransaction& transaction) {
            catalog::CatalogAccessor::Config accessor_cfg{};
            accessor_cfg.transaction = &transaction;
            accessor_cfg.scanner = storage_.make_scanner();
            return std::make_unique<catalog::CatalogAccessor>(accessor_cfg);
        };
        dispatcher_cfg.identifier_allocator = &allocator_;
        dispatcher_cfg.transaction_manager = &txn_manager_;
        dispatcher_cfg.commit_lsn_provider = [] { return 0ULL; };

        dispatcher_ = std::make_unique<ddl::DdlCommandDispatcher>(dispatcher_cfg);
        ddl::register_catalog_handlers(*dispatcher_);

        parser::DdlCommandBuilderConfig builder_cfg{};
        builder_cfg.default_database_id = catalog::kSystemDatabaseId;
        builder_cfg.default_schema_id = default_schema_id_;

        parser::DdlScriptExecutor::Config executor_cfg{};
        executor_cfg.builder_config = builder_cfg;
        executor_cfg.dispatcher = dispatcher_.get();
        executor_cfg.telemetry_identifier = "shell/integration";
        executor_cfg.storage_registry = &storage_registry_;

        executor_ = std::make_unique<parser::DdlScriptExecutor>(executor_cfg);

        shell_config_.ddl_executor = executor_.get();
        engine_ = std::make_unique<shell::ShellEngine>(shell_config_);
    }

    shell::CommandMetrics execute(const std::string& sql)
    {
        return engine_->execute_sql(sql);
    }

    [[nodiscard]] std::vector<catalog::CatalogTableView> tables() const
    {
        return storage_.list_tables();
    }

    [[nodiscard]] std::vector<catalog::CatalogColumnView> columns() const
    {
        return storage_.list_columns();
    }

    [[nodiscard]] catalog::SchemaId default_schema() const noexcept
    {
        return default_schema_id_;
    }

private:
    InMemoryCatalogStorage storage_{};
    txn::TransactionIdAllocatorStub txn_allocator_{1'000U};
    txn::SnapshotManagerStub snapshot_manager_;
    txn::TransactionManager txn_manager_;
    IdentifierAllocator allocator_{};
    storage::StorageTelemetryRegistry storage_registry_{};
    std::unique_ptr<ddl::DdlCommandDispatcher> dispatcher_{};
    std::unique_ptr<parser::DdlScriptExecutor> executor_{};
    shell::ShellEngine::Config shell_config_{};
    std::unique_ptr<shell::ShellEngine> engine_{};
    catalog::SchemaId default_schema_id_{catalog::SchemaId{42U}};
};

}  // namespace

TEST_CASE("ShellEngine executes DDL end-to-end via catalog handlers")
{
    ShellHarness harness;

    const auto metrics = harness.execute("CREATE TABLE metrics (id INT, name TEXT);");

    REQUIRE(metrics.success);
    CHECK(metrics.summary.find("dispatched") != std::string::npos);

    const auto tables = harness.tables();
    REQUIRE(tables.size() == 1U);
    const auto& table = tables.front();
    CHECK(table.schema_id == harness.default_schema());
    CHECK(table.name == "metrics");

    const auto columns = harness.columns();
    REQUIRE(columns.size() >= 2U);
    auto id_column = std::find_if(columns.begin(), columns.end(), [&](const catalog::CatalogColumnView& column) {
        return column.name == "id";
    });
    REQUIRE(id_column != columns.end());
    CHECK(id_column->relation_id == table.relation_id);

    auto name_column = std::find_if(columns.begin(), columns.end(), [&](const catalog::CatalogColumnView& column) {
        return column.name == "name";
    });
    REQUIRE(name_column != columns.end());
    CHECK(name_column->relation_id == table.relation_id);
}
