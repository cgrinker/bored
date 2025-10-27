#include "bored/parser/ddl_script_executor.hpp"

#include "bored/ddl/ddl_dispatcher.hpp"
#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/parser/ddl_command_builder.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/txn/transaction_manager.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <vector>

using namespace bored;
using namespace bored::parser;

namespace {

struct StubAllocator final : catalog::CatalogIdentifierAllocator {
    catalog::SchemaId allocate_schema_id() override
    {
        return catalog::SchemaId{++schema_ids};
    }

    catalog::RelationId allocate_table_id() override
    {
        return catalog::RelationId{++table_ids};
    }

    catalog::IndexId allocate_index_id() override
    {
        return catalog::IndexId{++index_ids};
    }

    catalog::ColumnId allocate_column_id() override
    {
        return catalog::ColumnId{++column_ids};
    }

    std::uint64_t schema_ids = 1'000U;
    std::uint64_t table_ids = 2'000U;
    std::uint64_t index_ids = 3'000U;
    std::uint64_t column_ids = 4'000U;
};

struct DispatcherHarness final {
    DispatcherHarness()
        : transaction_factory{[this](txn::TransactionContext* ctx) {
              catalog::CatalogTransactionConfig cfg{&txn_allocator, &snapshot_manager};
              auto transaction = std::make_unique<catalog::CatalogTransaction>(cfg);
              transaction->bind_transaction_context(&txn_manager, ctx);
              return transaction;
          }}
        , mutator_factory{[](catalog::CatalogTransaction& tx) {
              catalog::CatalogMutatorConfig cfg{};
              cfg.transaction = &tx;
              return std::make_unique<catalog::CatalogMutator>(cfg);
          }}
        , accessor_factory{[](catalog::CatalogTransaction& tx) {
              catalog::CatalogAccessor::Config cfg{};
              cfg.transaction = &tx;
              cfg.scanner = [](catalog::RelationId, const catalog::CatalogAccessor::TupleCallback&) {};
              return std::make_unique<catalog::CatalogAccessor>(cfg);
          }}
    {
    }

    txn::TransactionIdAllocatorStub txn_allocator{1'000U};
    txn::SnapshotManagerStub snapshot_manager{};
    txn::TransactionManager txn_manager{txn_allocator};
    StubAllocator allocator{};

    std::function<std::unique_ptr<catalog::CatalogTransaction>(txn::TransactionContext*)> transaction_factory;
    std::function<std::unique_ptr<catalog::CatalogMutator>(catalog::CatalogTransaction&)> mutator_factory;
    std::function<std::unique_ptr<catalog::CatalogAccessor>(catalog::CatalogTransaction&)> accessor_factory;
};

DdlCommandBuilderConfig make_builder_config()
{
    DdlCommandBuilderConfig config{};
    config.default_database_id = catalog::DatabaseId{42U};
    config.default_schema_id = catalog::SchemaId{84U};
    return config;
}

}  // namespace

TEST_CASE("DdlScriptExecutor dispatches translated commands")
{
    DispatcherHarness harness;
    ddl::DdlCommandDispatcher dispatcher({
        .transaction_factory = harness.transaction_factory,
        .mutator_factory = harness.mutator_factory,
        .accessor_factory = harness.accessor_factory,
        .identifier_allocator = &harness.allocator,
        .transaction_manager = &harness.txn_manager
    });

    storage::StorageTelemetryRegistry storage_registry;
    ParserTelemetryRegistry parser_registry;

    bool create_called = false;
    bool drop_called = false;

    dispatcher.register_handler<ddl::CreateTableRequest>([&](ddl::DdlCommandContext&, const ddl::CreateTableRequest& request) {
        create_called = true;
        CHECK(request.name == "metrics");
        REQUIRE(request.columns.size() == 1U);
        CHECK(request.columns.front().name == "id");
        return ddl::make_success();
    });

    dispatcher.register_handler<ddl::DropTableRequest>([&](ddl::DdlCommandContext&, const ddl::DropTableRequest& request) {
        drop_called = true;
        CHECK(request.name == "metrics");
        return ddl::make_success();
    });

    DdlScriptExecutor executor({
        .builder_config = make_builder_config(),
        .dispatcher = &dispatcher,
        .telemetry_registry = &parser_registry,
        .storage_registry = &storage_registry,
        .telemetry_identifier = "parser/integration"
    });

    const auto result = executor.execute("CREATE TABLE metrics (id INT); DROP TABLE metrics;");

    REQUIRE(result.builder.commands.size() == 2U);
    REQUIRE(result.responses.size() == 2U);
    CHECK(result.responses[0].success);
    CHECK(result.responses[1].success);
    CHECK(result.diagnostics.empty());
    CHECK(create_called);
    CHECK(drop_called);

    const auto snapshot = executor.telemetry().snapshot();
    CHECK(snapshot.scripts_attempted == 1U);
    CHECK(snapshot.scripts_succeeded == 1U);
    CHECK(snapshot.statements_attempted == 2U);
    CHECK(snapshot.statements_succeeded == 2U);
    CHECK(snapshot.diagnostics_error == 0U);

    const auto aggregated_parser = parser_registry.aggregate();
    CHECK(aggregated_parser.scripts_attempted == 1U);

    const auto aggregated_storage = storage_registry.aggregate_parser();
    CHECK(aggregated_storage.scripts_attempted == 1U);

    std::vector<std::string> ids;
    storage_registry.visit_parser([&](const std::string& id, const ParserTelemetrySnapshot& snapshot) {
        ids.push_back(id);
        CHECK(snapshot.scripts_attempted == 1U);
    });
    REQUIRE(ids.size() == 1U);
    CHECK(ids.front() == "parser/integration");
}

TEST_CASE("DdlScriptExecutor skips dispatch when translation reports errors")
{
    DispatcherHarness harness;
    ddl::DdlCommandDispatcher dispatcher({
        .transaction_factory = harness.transaction_factory,
        .mutator_factory = harness.mutator_factory,
        .accessor_factory = harness.accessor_factory,
        .identifier_allocator = &harness.allocator,
        .transaction_manager = &harness.txn_manager
    });

    bool handler_invoked = false;
    dispatcher.register_handler<ddl::CreateTableRequest>([&](ddl::DdlCommandContext&, const ddl::CreateTableRequest&) {
        handler_invoked = true;
        return ddl::make_success();
    });

    DdlScriptExecutor executor({
        .builder_config = make_builder_config(),
        .dispatcher = &dispatcher,
        .telemetry_identifier = ""
    });

    const auto result = executor.execute("CREATE TABLE metrics (value GEOMETRY);");

    CHECK(result.responses.empty());
    CHECK_FALSE(result.diagnostics.empty());
    const auto error_count = std::count_if(result.diagnostics.begin(), result.diagnostics.end(), [](const ParserDiagnostic& diagnostic) {
        return diagnostic.severity == ParserSeverity::Error;
    });
    CHECK(error_count >= 1);
    CHECK_FALSE(handler_invoked);

    const auto snapshot = executor.telemetry().snapshot();
    CHECK(snapshot.scripts_attempted == 1U);
    CHECK(snapshot.scripts_succeeded == 0U);
    CHECK(snapshot.diagnostics_error >= 1U);
}
