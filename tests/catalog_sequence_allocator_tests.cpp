#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/catalog/sequence_allocator.hpp"

#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <map>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using namespace bored::catalog;

struct InMemoryCatalogStorage final {
    using Relation = std::map<std::uint64_t, std::vector<std::byte>>;

    void seed(RelationId relation_id, std::uint64_t row_id, std::vector<std::byte> payload)
    {
        relations_[relation_id.value][row_id] = std::move(payload);
    }

    void apply(const CatalogMutationBatch& batch)
    {
        for (const auto& mutation : batch.mutations) {
            auto& relation = relations_[mutation.relation_id.value];
            switch (mutation.kind) {
            case CatalogMutationKind::Insert:
                relation[mutation.row_id] = mutation.after->payload;
                break;
            case CatalogMutationKind::Update:
                relation[mutation.row_id] = mutation.after->payload;
                break;
            case CatalogMutationKind::Delete:
                relation.erase(mutation.row_id);
                break;
            }
        }
    }

    [[nodiscard]] CatalogAccessor::RelationScanner make_scanner() const
    {
        return [this](RelationId relation_id, const CatalogAccessor::TupleCallback& callback) {
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

    std::unordered_map<std::uint64_t, Relation> relations_{};
};

[[nodiscard]] bored::txn::Snapshot relaxed_snapshot() noexcept
{
    bored::txn::Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 1'000U;
    return snapshot;
}

}  // namespace

TEST_CASE("Sequence allocator advances next value on commit")
{
    CatalogCache::instance().reset();

    InMemoryCatalogStorage storage;

    CatalogSequenceDescriptor base_descriptor{};
    base_descriptor.tuple.xmin = 1U;
    base_descriptor.sequence_id = SequenceId{10'000U};
    base_descriptor.schema_id = kSystemSchemaId;
    base_descriptor.owning_relation_id = RelationId{20'000U};
    base_descriptor.owning_column_id = ColumnId{30'000U};
    base_descriptor.start_value = 1U;
    base_descriptor.next_value = 1U;
    base_descriptor.increment = 1;
    base_descriptor.min_value = 1U;
    base_descriptor.max_value = 32U;
    base_descriptor.cache_size = 1U;
    base_descriptor.cycle = false;
    base_descriptor.name = "seq_alloc";

    storage.seed(kCatalogSequencesRelationId,
                 base_descriptor.sequence_id.value,
                 serialize_catalog_sequence(base_descriptor));

    bored::txn::TransactionIdAllocatorStub allocator{2'000U};
    bored::txn::SnapshotManagerStub snapshot_manager{relaxed_snapshot()};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogAccessor accessor({&transaction, storage.make_scanner()});
    CatalogMutator mutator({&transaction});
    mutator.set_publish_listener([&storage](const CatalogMutationBatch& batch) -> std::error_code {
        storage.apply(batch);
        return {};
    });

    SequenceAllocator allocator_component({&transaction, &accessor, &mutator});

    const auto first = allocator_component.allocate(base_descriptor.sequence_id);
    CHECK(first == 1U);

    const auto second = allocator_component.allocate(base_descriptor.sequence_id);
    CHECK(second == 2U);
    CHECK(allocator_component.has_pending_updates());

    REQUIRE_FALSE(transaction.commit());
    REQUIRE(mutator.has_published_batch());
    auto batch = mutator.consume_published_batch();
    storage.apply(batch);

    const auto& relation = storage.relations_[kCatalogSequencesRelationId.value];
    auto payload_it = relation.find(base_descriptor.sequence_id.value);
    REQUIRE(payload_it != relation.end());

    auto updated_view = decode_catalog_sequence(std::span<const std::byte>(payload_it->second.data(), payload_it->second.size()));
    REQUIRE(updated_view);
    CHECK(updated_view->next_value == 3U);
    CHECK(updated_view->start_value == base_descriptor.start_value);
    CHECK(updated_view->schema_id == base_descriptor.schema_id);

    CHECK_FALSE(allocator_component.has_pending_updates());
}

TEST_CASE("Sequence allocator stops at max value without cycling")
{
    CatalogCache::instance().reset();

    InMemoryCatalogStorage storage;

    CatalogSequenceDescriptor descriptor{};
    descriptor.tuple.xmin = 1U;
    descriptor.sequence_id = SequenceId{11'000U};
    descriptor.schema_id = kSystemSchemaId;
    descriptor.owning_relation_id = RelationId{21'000U};
    descriptor.owning_column_id = ColumnId{31'000U};
    descriptor.start_value = 5U;
    descriptor.next_value = 5U;
    descriptor.increment = 1;
    descriptor.min_value = 1U;
    descriptor.max_value = 5U;
    descriptor.cache_size = 1U;
    descriptor.cycle = false;
    descriptor.name = "seq_limit";

    storage.seed(kCatalogSequencesRelationId,
                 descriptor.sequence_id.value,
                 serialize_catalog_sequence(descriptor));

    bored::txn::TransactionIdAllocatorStub allocator{3'000U};
    bored::txn::SnapshotManagerStub snapshot_manager{relaxed_snapshot()};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogAccessor accessor({&transaction, storage.make_scanner()});
    CatalogMutator mutator({&transaction});
    mutator.set_publish_listener([&storage](const CatalogMutationBatch& batch) -> std::error_code {
        storage.apply(batch);
        return {};
    });

    SequenceAllocator allocator_component({&transaction, &accessor, &mutator});

    const auto final_value = allocator_component.allocate(descriptor.sequence_id);
    CHECK(final_value == descriptor.max_value);

    CHECK_THROWS_AS(allocator_component.allocate(descriptor.sequence_id), std::system_error);
}
