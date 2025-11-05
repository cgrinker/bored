#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/ddl/ddl_handlers.hpp"
#include "bored/parser/ddl_command_builder.hpp"
#include "bored/parser/ddl_script_executor.hpp"
#include "bored/executor/spool_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/executor/worktable_registry.hpp"
#include "bored/shell/shell_backend.hpp"
#include "bored/shell/shell_engine.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/txn/transaction_manager.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <initializer_list>
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

namespace bored::shell {

struct ShellBackendTestAccess final {
    static bored::executor::WorkTableRegistry& worktable_registry(ShellBackend& backend)
    {
        return backend.worktable_registry_;
    }
};

}  // namespace bored::shell

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

namespace {

std::vector<std::byte> encode_uint32(std::uint32_t value)
{
    std::vector<std::byte> bytes(sizeof(value));
    std::memcpy(bytes.data(), &value, sizeof(value));
    return bytes;
}

std::uint32_t decode_uint32(const std::span<const std::byte>& bytes)
{
    std::uint32_t value = 0U;
    REQUIRE(bytes.size() == sizeof(value));
    std::memcpy(&value, bytes.data(), sizeof(value));
    return value;
}

class BackendChildExecutor final : public executor::ExecutorNode {
public:
    explicit BackendChildExecutor(std::vector<std::vector<std::byte>> rows)
        : rows_{std::move(rows)}
    {
    }

    void open(executor::ExecutorContext&) override
    {
        ++open_count_;
        index_ = 0U;
    }

    bool next(executor::ExecutorContext&, executor::TupleBuffer& buffer) override
    {
        if (index_ >= rows_.size()) {
            return false;
        }

        executor::TupleWriter writer{buffer};
        writer.reset();
        writer.append_column(std::span<const std::byte>(rows_[index_].data(), rows_[index_].size()), false);
        writer.finalize();
        ++index_;
        ++produced_count_;
        return true;
    }

    void close(executor::ExecutorContext&) override
    {
        ++close_count_;
    }

    [[nodiscard]] std::size_t open_count() const noexcept { return open_count_; }
    [[nodiscard]] std::size_t produced_count() const noexcept { return produced_count_; }

private:
    std::vector<std::vector<std::byte>> rows_{};
    std::size_t index_ = 0U;
    std::size_t open_count_ = 0U;
    std::size_t close_count_ = 0U;
    std::size_t produced_count_ = 0U;
};

struct ShellStatementResult final {
    std::vector<std::uint32_t> observed;
    std::size_t child_open_count = 0U;
    std::size_t child_produced_count = 0U;
};

executor::TupleBuffer make_tuple_buffer(std::uint32_t value)
{
    executor::TupleBuffer buffer{};
    executor::TupleWriter writer{buffer};
    writer.reset();
    auto bytes = encode_uint32(value);
    writer.append_column(std::span<const std::byte>(bytes.data(), bytes.size()), false);
    writer.finalize();
    return buffer;
}

ShellStatementResult run_backend_statement(executor::WorkTableRegistry& registry,
                                           txn::Snapshot snapshot,
                                           std::uint64_t worktable_id,
                                           std::initializer_list<std::uint32_t> child_values,
                                           std::optional<std::uint32_t> appended_delta)
{
    std::vector<std::vector<std::byte>> encoded;
    encoded.reserve(child_values.size());
    for (auto value : child_values) {
        encoded.push_back(encode_uint32(value));
    }

    auto child = std::make_unique<BackendChildExecutor>(std::move(encoded));
    auto* raw_child = child.get();

    executor::SpoolExecutor::Config config{};
    config.worktable_registry = &registry;
    config.worktable_id = worktable_id;
    config.enable_recursive_cursor = true;

    executor::SpoolExecutor spool{std::move(child), config};

    executor::ExecutorContext context{};
    context.set_snapshot(snapshot);

    executor::TupleBuffer buffer{};
    ShellStatementResult result{};

    spool.open(context);
    while (spool.next(context, buffer)) {
        auto view = executor::TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 1U);
        const auto column = view.column(0U);
        REQUIRE_FALSE(column.is_null);
        result.observed.push_back(decode_uint32(column.data));
        buffer.reset();
    }
    spool.close(context);

    result.child_open_count = raw_child->open_count();
    result.child_produced_count = raw_child->produced_count();

    if (appended_delta.has_value()) {
        auto cursor_opt = spool.recursive_cursor();
        REQUIRE(cursor_opt.has_value());
        auto cursor = std::move(*cursor_opt);
        executor::TupleBuffer delta = make_tuple_buffer(*appended_delta);
        cursor.append_delta(std::move(delta));
        cursor.mark_delta_processed();
    }

    return result;
}

}  // namespace

TEST_CASE("ShellBackend shares recursive spool deltas across statements")
{
    shell::ShellBackend backend;
    auto& registry = shell::ShellBackendTestAccess::worktable_registry(backend);

    const std::uint64_t worktable_id = 0xACCE55U;

    txn::Snapshot snapshot{};
    snapshot.read_lsn = 512U;
    snapshot.xmin = 11U;
    snapshot.xmax = 4096U;

    auto first = run_backend_statement(registry, snapshot, worktable_id, {1U, 2U}, 3U);
    CHECK(first.observed == std::vector<std::uint32_t>{1U, 2U});
    CHECK(first.child_open_count == 1U);
    CHECK(first.child_produced_count == 2U);

    auto second = run_backend_statement(registry, snapshot, worktable_id, {}, 4U);
    CHECK(second.observed == std::vector<std::uint32_t>{1U, 2U, 3U});
    CHECK(second.child_open_count == 0U);
    CHECK(second.child_produced_count == 0U);

    auto third = run_backend_statement(registry, snapshot, worktable_id, {}, std::nullopt);
    CHECK(third.observed == std::vector<std::uint32_t>{1U, 2U, 3U, 4U});
    CHECK(third.child_open_count == 0U);
    CHECK(third.child_produced_count == 0U);
}
