#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_transaction.hpp"

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

TEST_CASE("CatalogAccessor enumerates relation constraints")
{
    CatalogCache::instance().reset();

    InMemoryCatalogStorage storage;

    CatalogConstraintDescriptor descriptor{};
    descriptor.tuple.xmin = 1U;
    descriptor.constraint_id = ConstraintId{42U};
    descriptor.relation_id = RelationId{200U};
    descriptor.constraint_type = CatalogConstraintType::PrimaryKey;
    descriptor.backing_index_id = IndexId{300U};
    descriptor.referenced_relation_id = RelationId{0U};
    descriptor.key_columns = "id";
    descriptor.referenced_columns = "";
    descriptor.name = "pk_items";

    storage.seed(kCatalogConstraintsRelationId,
                 descriptor.constraint_id.value,
                 serialize_catalog_constraint(descriptor));

    bored::txn::TransactionIdAllocatorStub allocator{100U};
    bored::txn::SnapshotManagerStub snapshot_manager{relaxed_snapshot()};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogAccessor accessor({&transaction, storage.make_scanner()});

    SECTION("Lookup by identifier")
    {
        auto found = accessor.constraint(descriptor.constraint_id);
        REQUIRE(found.has_value());
        CHECK(found->constraint_type == descriptor.constraint_type);
        CHECK(found->relation_id == descriptor.relation_id);
        CHECK(found->backing_index_id == descriptor.backing_index_id);
        CHECK(found->key_columns == descriptor.key_columns);
        CHECK(found->name == descriptor.name);
    }

    SECTION("Relation-scoped enumeration")
    {
        auto relation_constraints = accessor.constraints(descriptor.relation_id);
        REQUIRE(relation_constraints.size() == 1U);
        CHECK(relation_constraints.front().constraint_id == descriptor.constraint_id);
    }

    SECTION("Global enumeration")
    {
        auto all_constraints = accessor.constraints();
        REQUIRE(all_constraints.size() == 1U);
        CHECK(all_constraints.front().name == descriptor.name);
    }

    SECTION("Missing lookups return empty results")
    {
        CHECK_FALSE(accessor.constraint(ConstraintId{1337U}).has_value());
        CHECK(accessor.constraints(RelationId{descriptor.relation_id.value + 1U}).empty());
    }
}
