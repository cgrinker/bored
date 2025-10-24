#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_mvcc.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/storage/wal_format.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <string_view>
#include <vector>
#include <stdexcept>
#include <system_error>

namespace {

using namespace bored::catalog;

std::vector<std::byte> make_payload(std::string_view name)
{
    CatalogTableDescriptor descriptor{};
    descriptor.tuple.xmin = 100U;
    descriptor.relation_id = kCatalogTablesRelationId;
    descriptor.schema_id = kSystemSchemaId;
    descriptor.name = name;
    return serialize_catalog_table(descriptor);
}

}  // namespace

TEST_CASE("Catalog tuple builder produces expected metadata")
{
    bored::txn::TransactionIdAllocatorStub allocator{500U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    SECTION("Insert descriptor")
    {
        auto descriptor = CatalogTupleBuilder::for_insert(transaction);
        CHECK(descriptor.xmin == transaction.transaction_id());
        CHECK(descriptor.xmax == 0U);
    }

    SECTION("Update descriptor overrides xmin")
    {
        CatalogTupleDescriptor existing{};
        existing.xmin = 42U;
        existing.xmax = 0U;
        auto descriptor = CatalogTupleBuilder::for_update(transaction, existing);
        CHECK(descriptor.xmin == transaction.transaction_id());
        CHECK(descriptor.xmax == 0U);
    }

    SECTION("Delete descriptor stamps xmax")
    {
        CatalogTupleDescriptor existing{};
        existing.xmin = 12U;
        existing.xmax = 0U;
        auto descriptor = CatalogTupleBuilder::for_delete(transaction, existing);
        CHECK(descriptor.xmin == existing.xmin);
        CHECK(descriptor.xmax == transaction.transaction_id());
    }
}

TEST_CASE("Catalog mutator stages insert update and delete")
{
    bored::txn::TransactionIdAllocatorStub allocator{600U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogMutator mutator({&transaction});

    auto insert_descriptor = CatalogTupleBuilder::for_insert(transaction);
    mutator.stage_insert(kCatalogTablesRelationId, 9001U, insert_descriptor, make_payload("insert"));

    CatalogTupleDescriptor existing{};
    existing.xmin = 123U;
    existing.xmax = 0U;
    mutator.stage_update(kCatalogTablesRelationId,
                         9002U,
                         existing,
                         make_payload("before"),
                         CatalogTupleBuilder::for_update(transaction, existing),
                         make_payload("after"));

    mutator.stage_delete(kCatalogTablesRelationId, 9003U, existing, make_payload("delete"));

    const auto& staged = mutator.staged_mutations();
    const auto& wal_records = mutator.staged_wal_records();
    REQUIRE(staged.size() == 3U);
    REQUIRE(wal_records.size() == staged.size());
    CHECK_FALSE(mutator.has_published_batch());

    CHECK(staged[0].kind == CatalogMutationKind::Insert);
    CHECK(staged[0].relation_id == kCatalogTablesRelationId);
    REQUIRE(staged[0].after);
    CHECK_FALSE(staged[0].before);
    CHECK(staged[0].after->descriptor.xmin == transaction.transaction_id());

    CHECK(staged[1].kind == CatalogMutationKind::Update);
    REQUIRE(staged[1].before);
    REQUIRE(staged[1].after);
    CHECK(staged[1].before->descriptor.xmin == existing.xmin);
    CHECK(staged[1].after->descriptor.xmin == transaction.transaction_id());

    CHECK(staged[2].kind == CatalogMutationKind::Delete);
    REQUIRE(staged[2].before);
    CHECK_FALSE(staged[2].after);
    CHECK(staged[2].before->descriptor.xmin == existing.xmin);

    CHECK_FALSE(wal_records[0].has_value());
    CHECK_FALSE(wal_records[1].has_value());
    CHECK_FALSE(wal_records[2].has_value());

    mutator.clear();
    CHECK(mutator.empty());
    CHECK(mutator.staged_wal_records().empty());
    CHECK_FALSE(mutator.has_published_batch());
}

TEST_CASE("Catalog mutator wal staging aligns with tuple mutations")
{
    bored::txn::TransactionIdAllocatorStub allocator{701U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogMutator mutator({&transaction});

    auto descriptor = CatalogTupleBuilder::for_insert(transaction);
    mutator.stage_insert(kCatalogTablesRelationId, 11000U, descriptor, make_payload("payload"));

    REQUIRE(mutator.staged_mutations().size() == 1U);
    REQUIRE(mutator.staged_wal_records().size() == 1U);
    CHECK_FALSE(mutator.staged_wal_records()[0].has_value());
    CHECK_FALSE(mutator.has_published_batch());

    auto& wal = mutator.ensure_wal_record(0);
    wal.type = bored::storage::WalRecordType::CatalogInsert;
    wal.flags = bored::storage::WalRecordFlag::HasPayload;
    wal.page_id = kCatalogTablesPageId;
    wal.payload = mutator.staged_mutations()[0].after->payload;

    const auto& wal_records = mutator.staged_wal_records();
    REQUIRE(wal_records[0].has_value());
    CHECK(wal_records[0]->type == bored::storage::WalRecordType::CatalogInsert);
    CHECK(wal_records[0]->flags == bored::storage::WalRecordFlag::HasPayload);
    CHECK(wal_records[0]->page_id == kCatalogTablesPageId);
    CHECK(wal_records[0]->payload.size() == mutator.staged_mutations()[0].after->payload.size());
    CHECK_FALSE(mutator.has_published_batch());

    SECTION("clear wal record resets entry")
    {
        mutator.clear_wal_record(0);
        CHECK_FALSE(mutator.staged_wal_records()[0].has_value());
    }

    SECTION("ensure wal record bounds validation")
    {
        CHECK_THROWS_AS(mutator.ensure_wal_record(1), std::out_of_range);
        CHECK_THROWS_AS(mutator.wal_record(1), std::out_of_range);
    }
}

TEST_CASE("Catalog mutator publishes staged batch on commit")
{
    bored::txn::TransactionIdAllocatorStub allocator{801U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogMutator mutator({&transaction});

    auto descriptor = CatalogTupleBuilder::for_insert(transaction);
    mutator.stage_insert(kCatalogTablesRelationId, 12000U, descriptor, make_payload("commit"));

    REQUIRE(mutator.staged_mutations().size() == 1U);
    REQUIRE_FALSE(mutator.has_published_batch());

    auto commit_ec = transaction.commit();
    REQUIRE_FALSE(commit_ec);
    CHECK(transaction.is_committed());
    CHECK(mutator.empty());
    REQUIRE(mutator.has_published_batch());

    const auto& batch = mutator.published_batch();
    REQUIRE(batch.mutations.size() == 1U);
    CHECK(batch.mutations[0].row_id == 12000U);
    CHECK(batch.mutations[0].relation_id == kCatalogTablesRelationId);
    REQUIRE(batch.wal_records.size() == 1U);
    CHECK_FALSE(batch.wal_records[0].has_value());

    auto consumed = mutator.consume_published_batch();
    CHECK(consumed.mutations.size() == 1U);
    CHECK(consumed.wal_records.size() == 1U);
    CHECK_FALSE(mutator.has_published_batch());
}

TEST_CASE("Catalog mutator abort discards staged batch")
{
    bored::txn::TransactionIdAllocatorStub allocator{901U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogMutator mutator({&transaction});

    auto descriptor = CatalogTupleBuilder::for_insert(transaction);
    mutator.stage_insert(kCatalogTablesRelationId, 13000U, descriptor, make_payload("abort"));

    REQUIRE(mutator.staged_mutations().size() == 1U);

    auto abort_ec = transaction.abort();
    REQUIRE_FALSE(abort_ec);
    CHECK(transaction.is_aborted());
    CHECK(mutator.empty());
    CHECK_FALSE(mutator.has_published_batch());
}
