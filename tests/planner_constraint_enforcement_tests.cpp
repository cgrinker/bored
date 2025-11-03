#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/planner/planner.hpp"
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

namespace catalog = bored::catalog;
namespace planner = bored::planner;
namespace txn = bored::txn;

namespace {

struct InMemoryCatalogStorage final {
    using Relation = std::map<std::uint64_t, std::vector<std::byte>>;

    void seed(catalog::RelationId relation_id, std::uint64_t row_id, std::vector<std::byte> payload)
    {
        relations_[relation_id.value][row_id] = std::move(payload);
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

    std::unordered_map<std::uint64_t, Relation> relations_{};
};

[[nodiscard]] txn::Snapshot relaxed_snapshot() noexcept
{
    txn::Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 1'000U;
    return snapshot;
}

planner::LogicalOperatorPtr make_insert_plan(catalog::RelationId relation_id,
                                             std::vector<std::string> output_columns)
{
    planner::LogicalProperties insert_props{};
    insert_props.relation_name = "analytics.synthetic";
    insert_props.relation_id = relation_id;
    insert_props.output_columns = std::move(output_columns);

    auto values = planner::LogicalOperator::make(planner::LogicalOperatorType::Values);
    return planner::LogicalOperator::make(
        planner::LogicalOperatorType::Insert,
        std::vector<planner::LogicalOperatorPtr>{values},
        insert_props);
}

}  // namespace

TEST_CASE("Planner inserts unique enforcement before values when primary key exists")
{
    catalog::CatalogCache::instance().reset();

    InMemoryCatalogStorage storage;

    catalog::CatalogConstraintDescriptor pk{};
    pk.tuple.xmin = 1U;
    pk.constraint_id = catalog::ConstraintId{101U};
    pk.relation_id = catalog::RelationId{200U};
    pk.constraint_type = catalog::CatalogConstraintType::PrimaryKey;
    pk.backing_index_id = catalog::IndexId{301U};
    pk.key_columns = "id";
    pk.referenced_columns = "";
    pk.name = "pk_items";

    storage.seed(catalog::kCatalogConstraintsRelationId,
                 pk.constraint_id.value,
                 catalog::serialize_catalog_constraint(pk));

    txn::TransactionIdAllocatorStub allocator{100U};
    txn::SnapshotManagerStub snapshot_manager{relaxed_snapshot()};
    catalog::CatalogTransaction transaction({&allocator, &snapshot_manager});
    catalog::CatalogAccessor accessor({&transaction, storage.make_scanner()});

    auto insert = make_insert_plan(pk.relation_id, {"id"});
    planner::PlannerContextConfig config{};
    config.catalog = &accessor;
    planner::PlannerContext context{config};

    auto result = planner::plan_query(context, planner::LogicalPlan{insert});
    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == planner::PhysicalOperatorType::Insert);
    REQUIRE(root->children().size() == 1U);
    auto enforcement = root->children().front();
    REQUIRE(enforcement);
    CHECK(enforcement->type() == planner::PhysicalOperatorType::UniqueEnforce);
    REQUIRE(enforcement->properties().unique_enforcement.has_value());
    const auto& unique_props = *enforcement->properties().unique_enforcement;
    CHECK(unique_props.constraint_id == pk.constraint_id);
    CHECK(unique_props.is_primary_key);
    CHECK(unique_props.key_columns == std::vector<std::string>{"id"});
    REQUIRE(enforcement->children().size() == 1U);
    CHECK(enforcement->children().front()->type() == planner::PhysicalOperatorType::Values);
}

TEST_CASE("Planner stacks unique and foreign key enforcement for inserts")
{
    catalog::CatalogCache::instance().reset();

    InMemoryCatalogStorage storage;

    catalog::CatalogConstraintDescriptor pk{};
    pk.tuple.xmin = 1U;
    pk.constraint_id = catalog::ConstraintId{201U};
    pk.relation_id = catalog::RelationId{444U};
    pk.constraint_type = catalog::CatalogConstraintType::Unique;
    pk.backing_index_id = catalog::IndexId{555U};
    pk.key_columns = "email";
    pk.referenced_columns = "";
    pk.name = "users_email_key";

    catalog::CatalogConstraintDescriptor fk{};
    fk.tuple.xmin = 1U;
    fk.constraint_id = catalog::ConstraintId{202U};
    fk.relation_id = pk.relation_id;
    fk.constraint_type = catalog::CatalogConstraintType::ForeignKey;
    fk.backing_index_id = catalog::IndexId{0U};
    fk.referenced_relation_id = catalog::RelationId{900U};
    fk.key_columns = "role_id";
    fk.referenced_columns = "id";
    fk.name = "users_role_fk";

    storage.seed(catalog::kCatalogConstraintsRelationId,
                 pk.constraint_id.value,
                 catalog::serialize_catalog_constraint(pk));
    storage.seed(catalog::kCatalogConstraintsRelationId,
                 fk.constraint_id.value,
                 catalog::serialize_catalog_constraint(fk));

    txn::TransactionIdAllocatorStub allocator{400U};
    txn::SnapshotManagerStub snapshot_manager{relaxed_snapshot()};
    catalog::CatalogTransaction transaction({&allocator, &snapshot_manager});
    catalog::CatalogAccessor accessor({&transaction, storage.make_scanner()});

    auto insert = make_insert_plan(pk.relation_id, {"email", "role_id"});
    planner::PlannerContextConfig config{};
    config.catalog = &accessor;
    planner::PlannerContext context{config};

    auto result = planner::plan_query(context, planner::LogicalPlan{insert});
    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == planner::PhysicalOperatorType::Insert);
    REQUIRE(root->children().size() == 1U);
    auto unique_node = root->children().front();
    REQUIRE(unique_node);
    CHECK(unique_node->type() == planner::PhysicalOperatorType::UniqueEnforce);
    REQUIRE(unique_node->children().size() == 1U);
    auto fk_node = unique_node->children().front();
    REQUIRE(fk_node);
    CHECK(fk_node->type() == planner::PhysicalOperatorType::ForeignKeyCheck);
    REQUIRE(unique_node->properties().unique_enforcement.has_value());
    const auto& unique_props = *unique_node->properties().unique_enforcement;
    CHECK(unique_props.constraint_id == pk.constraint_id);
    CHECK_FALSE(unique_props.is_primary_key);
    CHECK(unique_props.key_columns == std::vector<std::string>{"email"});
    REQUIRE(fk_node->properties().foreign_key_enforcement.has_value());
    const auto& fk_props = *fk_node->properties().foreign_key_enforcement;
    CHECK(fk_props.constraint_id == fk.constraint_id);
    CHECK(fk_props.referenced_relation_id == fk.referenced_relation_id);
    CHECK(fk_props.referencing_columns == std::vector<std::string>{"role_id"});
    CHECK(fk_props.referenced_columns == std::vector<std::string>{"id"});
    REQUIRE(fk_node->children().size() == 1U);
    CHECK(fk_node->children().front()->type() == planner::PhysicalOperatorType::Values);
}
