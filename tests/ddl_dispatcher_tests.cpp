#include "bored/ddl/ddl_dispatcher.hpp"
#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <stdexcept>

using namespace bored;
using namespace bored::ddl;

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

    std::uint64_t schema_ids = 100U;
    std::uint64_t table_ids = 200U;
    std::uint64_t index_ids = 300U;
    std::uint64_t column_ids = 400U;
};

struct DispatcherHarness final {
    DispatcherHarness()
        : transaction_factory{[this] {
              catalog::CatalogTransactionConfig cfg{&txn_allocator, &snapshot_manager};
              return std::make_unique<catalog::CatalogTransaction>(cfg);
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

    txn::TransactionIdAllocatorStub txn_allocator{900U};
    txn::SnapshotManagerStub snapshot_manager{};
    StubAllocator allocator{};

    std::function<std::unique_ptr<catalog::CatalogTransaction>()> transaction_factory;
    std::function<std::unique_ptr<catalog::CatalogMutator>(catalog::CatalogTransaction&)> mutator_factory;
    std::function<std::unique_ptr<catalog::CatalogAccessor>(catalog::CatalogTransaction&)> accessor_factory;
};

CreateSchemaRequest make_create_schema()
{
    CreateSchemaRequest request{};
    request.database_id = catalog::DatabaseId{1U};
    request.name = "analytics";
    return request;
}

}  // namespace

TEST_CASE("DdlCommandDispatcher commits transaction on success")
{
    DispatcherHarness harness;
    DdlTelemetryRegistry registry;

    DdlCommandDispatcher dispatcher({
        .transaction_factory = harness.transaction_factory,
        .mutator_factory = harness.mutator_factory,
        .accessor_factory = harness.accessor_factory,
        .identifier_allocator = &harness.allocator,
        .commit_lsn_provider = [] { return 42ULL; },
        .telemetry_registry = &registry,
        .telemetry_identifier = "ddl"
    });

    bool committed = false;
    bool aborted = false;

    dispatcher.register_handler<CreateSchemaRequest>([&](DdlCommandContext& ctx, const CreateSchemaRequest&) {
        ctx.transaction.register_commit_hook([&]() -> std::error_code {
            committed = true;
            return {};
        });
        ctx.transaction.register_abort_hook([&]() { aborted = true; });
        return make_success();
    });

    DdlCommand command = make_create_schema();
    const auto response = dispatcher.dispatch(command);

    CHECK(response.success);
    CHECK(committed);
    CHECK_FALSE(aborted);

    const auto snapshot = dispatcher.telemetry().snapshot();
    const auto index = static_cast<std::size_t>(DdlVerb::CreateSchema);
    CHECK(snapshot.verbs[index].attempts == 1U);
    CHECK(snapshot.verbs[index].successes == 1U);
    CHECK(snapshot.verbs[index].failures == 0U);

    const auto aggregated = registry.aggregate();
    CHECK(aggregated.verbs[index].successes == 1U);
}

TEST_CASE("DdlCommandDispatcher aborts transaction on handler failure")
{
    DispatcherHarness harness;

    DdlCommandDispatcher dispatcher({
        .transaction_factory = harness.transaction_factory,
        .mutator_factory = harness.mutator_factory,
        .accessor_factory = harness.accessor_factory,
        .identifier_allocator = &harness.allocator
    });

    bool aborted = false;
    dispatcher.register_handler<CreateSchemaRequest>([&](DdlCommandContext& ctx, const CreateSchemaRequest&) {
        ctx.transaction.register_abort_hook([&]() { aborted = true; });
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "validation error");
    });

    DdlCommand command = make_create_schema();
    const auto response = dispatcher.dispatch(command);

    CHECK_FALSE(response.success);
    CHECK(response.error == make_error_code(DdlErrc::ValidationFailed));
    CHECK(aborted);

    const auto snapshot = dispatcher.telemetry().snapshot();
    const auto index = static_cast<std::size_t>(DdlVerb::CreateSchema);
    CHECK(snapshot.verbs[index].attempts == 1U);
    CHECK(snapshot.verbs[index].successes == 0U);
    CHECK(snapshot.verbs[index].failures == 1U);
    CHECK(snapshot.failures.validation_failures == 1U);
}

TEST_CASE("DdlCommandDispatcher reports handler exceptions")
{
    DispatcherHarness harness;

    DdlCommandDispatcher dispatcher({
        .transaction_factory = harness.transaction_factory,
        .mutator_factory = harness.mutator_factory,
        .accessor_factory = harness.accessor_factory,
        .identifier_allocator = &harness.allocator
    });

    dispatcher.register_handler<CreateSchemaRequest>([](DdlCommandContext&, const CreateSchemaRequest&) -> DdlCommandResponse {
        throw std::runtime_error{"handler blew up"};
    });

    DdlCommand command = make_create_schema();
    const auto response = dispatcher.dispatch(command);

    CHECK_FALSE(response.success);
    CHECK(response.error == make_error_code(DdlErrc::ExecutionFailed));
    CHECK(response.message == "handler blew up");

    const auto snapshot = dispatcher.telemetry().snapshot();
    const auto index = static_cast<std::size_t>(DdlVerb::CreateSchema);
    CHECK(snapshot.verbs[index].failures == 1U);
    CHECK(snapshot.failures.execution_failures == 1U);
}

TEST_CASE("DdlCommandDispatcher surfaces missing handlers")
{
    DispatcherHarness harness;

    DdlCommandDispatcher dispatcher({
        .transaction_factory = harness.transaction_factory,
        .mutator_factory = harness.mutator_factory,
        .accessor_factory = harness.accessor_factory,
        .identifier_allocator = &harness.allocator
    });

    DdlCommand command = make_create_schema();
    const auto response = dispatcher.dispatch(command);

    CHECK_FALSE(response.success);
    CHECK(response.error == make_error_code(DdlErrc::HandlerMissing));

    const auto snapshot = dispatcher.telemetry().snapshot();
    const auto index = static_cast<std::size_t>(DdlVerb::CreateSchema);
    CHECK(snapshot.verbs[index].attempts == 1U);
    CHECK(snapshot.verbs[index].failures == 1U);
    CHECK(snapshot.failures.handler_missing == 1U);
}
