#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_mvcc.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
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
    CatalogWalRecordFragment fragment{};
    fragment.type = bored::storage::WalRecordType::CatalogInsert;
    fragment.flags = bored::storage::WalRecordFlag::HasPayload;
    fragment.page_id = kCatalogTablesPageId;
    fragment.payload = mutator.staged_mutations()[0].after->payload;
    wal.records.push_back(std::move(fragment));

    const auto& wal_records = mutator.staged_wal_records();
    REQUIRE(wal_records[0].has_value());
    REQUIRE(wal_records[0]->records.size() == 1U);
    CHECK(wal_records[0]->records[0].type == bored::storage::WalRecordType::CatalogInsert);
    CHECK(wal_records[0]->records[0].flags == bored::storage::WalRecordFlag::HasPayload);
    CHECK(wal_records[0]->records[0].page_id == kCatalogTablesPageId);
    CHECK(wal_records[0]->records[0].payload.size() == mutator.staged_mutations()[0].after->payload.size());
    CHECK(wal_records[0]->commit_lsn == 0U);
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
    REQUIRE(batch.wal_records[0].has_value());
    CHECK(batch.commit_lsn == 0U);
    CHECK(batch.wal_records[0]->commit_lsn == batch.commit_lsn);
    REQUIRE(batch.wal_records[0]->records.size() == 1U);

    const auto& fragment = batch.wal_records[0]->records[0];
    CHECK(fragment.type == bored::storage::WalRecordType::CatalogInsert);
    auto meta = bored::storage::decode_wal_tuple_meta(std::span<const std::byte>(fragment.payload.data(), fragment.payload.size()));
    REQUIRE(meta);
    CHECK(meta->page_id == kCatalogTablesPageId);
    CHECK(meta->row_id == 12000U);
    auto expected_payload = make_payload("commit");
    CHECK(meta->tuple_length == expected_payload.size());

    auto consumed = mutator.consume_published_batch();
    CHECK(consumed.mutations.size() == 1U);
    CHECK(consumed.wal_records.size() == 1U);
    CHECK_FALSE(mutator.has_published_batch());
}

TEST_CASE("Catalog mutator commit captures before image for deletes")
{
    bored::txn::TransactionIdAllocatorStub allocator{811U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogMutator mutator({&transaction});

    CatalogTupleDescriptor existing{};
    existing.xmin = 42U;
    existing.xmax = 0U;
    auto before_payload = make_payload("drop");
    mutator.stage_delete(kCatalogTablesRelationId, 7777U, existing, before_payload);

    REQUIRE_FALSE(transaction.commit());

    const auto& batch = mutator.published_batch();
    REQUIRE(batch.wal_records.size() == 1U);
    REQUIRE(batch.wal_records[0].has_value());
    const auto& records = batch.wal_records[0]->records;
    REQUIRE(records.size() == 2U);
    CHECK(records[0].type == bored::storage::WalRecordType::TupleBeforeImage);
    CHECK(records[1].type == bored::storage::WalRecordType::CatalogDelete);

    auto before_view = bored::storage::decode_wal_tuple_before_image(std::span<const std::byte>(records[0].payload.data(), records[0].payload.size()));
    REQUIRE(before_view);
    CHECK(before_view->meta.page_id == kCatalogTablesPageId);
    CHECK(before_view->meta.row_id == 7777U);
    CHECK(before_view->tuple_payload.size() == before_payload.size());
    CHECK(std::equal(before_payload.begin(), before_payload.end(), before_view->tuple_payload.begin()));

    auto delete_meta = bored::storage::decode_wal_tuple_meta(std::span<const std::byte>(records[1].payload.data(), records[1].payload.size()));
    REQUIRE(delete_meta);
    CHECK(delete_meta->tuple_length == 0U);
    CHECK(delete_meta->row_id == 7777U);
}

TEST_CASE("Catalog mutator commit captures before image and update record")
{
    bored::txn::TransactionIdAllocatorStub allocator{821U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogMutator mutator({&transaction});

    CatalogTupleDescriptor existing{};
    existing.xmin = 55U;
    existing.xmax = 0U;
    auto before_payload = make_payload("old_name");
    auto after_payload = make_payload("new_name");
    mutator.stage_update(kCatalogTablesRelationId,
                         8888U,
                         existing,
                         before_payload,
                         CatalogTupleBuilder::for_update(transaction, existing),
                         after_payload);

    REQUIRE_FALSE(transaction.commit());

    const auto& batch = mutator.published_batch();
    REQUIRE(batch.wal_records.size() == 1U);
    REQUIRE(batch.wal_records[0].has_value());
    const auto& records = batch.wal_records[0]->records;
    REQUIRE(records.size() == 2U);
    CHECK(records[0].type == bored::storage::WalRecordType::TupleBeforeImage);
    CHECK(records[1].type == bored::storage::WalRecordType::CatalogUpdate);

    auto before_view = bored::storage::decode_wal_tuple_before_image(std::span<const std::byte>(records[0].payload.data(), records[0].payload.size()));
    REQUIRE(before_view);
    CHECK(before_view->meta.row_id == 8888U);
    CHECK(before_view->tuple_payload.size() == before_payload.size());

    auto update_meta = bored::storage::decode_wal_tuple_update_meta(std::span<const std::byte>(records[1].payload.data(), records[1].payload.size()));
    REQUIRE(update_meta);
    CHECK(update_meta->base.row_id == 8888U);
    CHECK(update_meta->base.tuple_length == after_payload.size());
    CHECK(update_meta->old_length == before_payload.size());
}

TEST_CASE("Catalog mutator binds commit lsn from provider")
{
    bored::txn::TransactionIdAllocatorStub allocator{831U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    const std::uint64_t expected_lsn = 0xABCDEFULL;
    CatalogMutator mutator({&transaction, [expected_lsn]() { return expected_lsn; }});

    auto descriptor = CatalogTupleBuilder::for_insert(transaction);
    mutator.stage_insert(kCatalogTablesRelationId, 14000U, descriptor, make_payload("lsn"));

    REQUIRE_FALSE(transaction.commit());

    const auto& batch = mutator.published_batch();
    CHECK(batch.commit_lsn == expected_lsn);
    REQUIRE(batch.wal_records[0].has_value());
    CHECK(batch.wal_records[0]->commit_lsn == expected_lsn);
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

TEST_CASE("Catalog mutator commit invalidates accessor caches")
{
    bored::txn::TransactionIdAllocatorStub allocator{911U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogMutator mutator({&transaction});

    auto descriptor = CatalogTupleBuilder::for_insert(transaction);
    mutator.stage_insert(kCatalogTablesRelationId, 15000U, descriptor, make_payload("invalidate"));

    const auto before_epoch = CatalogAccessor::current_epoch(kCatalogTablesRelationId);

    REQUIRE_FALSE(transaction.commit());

    const auto after_epoch = CatalogAccessor::current_epoch(kCatalogTablesRelationId);
    CHECK(after_epoch > before_epoch);
}

TEST_CASE("CatalogMutator telemetry tracks published aborted and failed batches")
{
    CatalogMutator::reset_telemetry();

    bored::txn::TransactionIdAllocatorStub allocator{10'000U};
    bored::txn::SnapshotManagerStub snapshot_manager{};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    const std::uint64_t commit_lsn = 0xFFU;
    CatalogMutator mutator({&transaction, [commit_lsn]() { return commit_lsn; }});

    CatalogTableDescriptor table_descriptor{};
    table_descriptor.tuple = CatalogTupleBuilder::for_insert(transaction);
    table_descriptor.relation_id = RelationId{12'345U};
    table_descriptor.schema_id = kSystemSchemaId;
    table_descriptor.table_type = CatalogTableType::Heap;
    table_descriptor.root_page_id = 777U;
    table_descriptor.name = "telemetry";
    auto payload = serialize_catalog_table(table_descriptor);

    mutator.stage_insert(kCatalogTablesRelationId, table_descriptor.relation_id.value, table_descriptor.tuple, std::move(payload));
    REQUIRE_FALSE(transaction.commit());

    auto telemetry = CatalogMutator::telemetry();
    REQUIRE(telemetry.published_batches == 1U);
    REQUIRE(telemetry.published_mutations == 1U);
    REQUIRE(telemetry.published_wal_records >= 1U);
    REQUIRE(telemetry.publish_failures == 0U);
    REQUIRE(telemetry.aborted_batches == 0U);

    bored::txn::TransactionIdAllocatorStub abort_allocator{11'000U};
    bored::txn::SnapshotManagerStub abort_snapshot_manager{};
    CatalogTransaction abort_tx({&abort_allocator, &abort_snapshot_manager});
    CatalogMutator abort_mutator({&abort_tx});
    auto abort_descriptor = CatalogTupleBuilder::for_insert(abort_tx);
    abort_mutator.stage_insert(kCatalogTablesRelationId, 22'000U, abort_descriptor, make_payload("abort"));
    REQUIRE_FALSE(abort_tx.abort());

    telemetry = CatalogMutator::telemetry();
    REQUIRE(telemetry.aborted_batches == 1U);
    REQUIRE(telemetry.aborted_mutations == 1U);

    bored::txn::TransactionIdAllocatorStub failure_allocator{12'000U};
    bored::txn::SnapshotManagerStub failure_snapshot_manager{};
    CatalogTransaction failure_tx({&failure_allocator, &failure_snapshot_manager});
    CatalogMutator failure_mutator({&failure_tx});
    auto failure_descriptor = CatalogTupleBuilder::for_insert(failure_tx);
    failure_mutator.stage_insert(RelationId{999'999U}, 30'000U, failure_descriptor, make_payload("fail"));

    auto failure_ec = failure_tx.commit();
    REQUIRE(failure_ec == std::make_error_code(std::errc::invalid_argument));

    telemetry = CatalogMutator::telemetry();
    REQUIRE(telemetry.publish_failures == 1U);
    REQUIRE(telemetry.published_batches == 1U);
    REQUIRE(telemetry.aborted_batches == 2U);
    REQUIRE(telemetry.aborted_mutations == 2U);
}
