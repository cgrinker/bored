#include "bored/catalog/catalog_introspection.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

TEST_CASE("Catalog introspection JSON encodes relations and indexes")
{
    using namespace bored::catalog;

    CatalogIntrospectionSnapshot snapshot{};

    CatalogRelationSummary relation{};
    relation.database_name = "system";
    relation.schema_name = "public";
    relation.relation_name = "metrics";
    relation.relation_kind = CatalogRelationKind::Table;
    relation.root_page_id = 123U;
    relation.column_count = 4U;
    snapshot.relations.push_back(relation);

    CatalogIndexSummary index{};
    index.schema_name = "public";
    index.relation_name = "metrics";
    index.index_name = "metrics_pk";
    index.index_type = CatalogIndexType::BTree;
    index.max_fanout = 64U;
    index.root_page_id = 456U;
    snapshot.indexes.push_back(index);

    const auto json = catalog_introspection_to_json(snapshot);
    using Catch::Matchers::ContainsSubstring;
    CHECK_THAT(json, ContainsSubstring("\"relations\""));
    CHECK_THAT(json, ContainsSubstring("metrics"));
    CHECK_THAT(json, ContainsSubstring("\"indexes\""));
}

TEST_CASE("Catalog introspection global sampler yields snapshot")
{
    using namespace bored::catalog;

    auto previous = get_global_catalog_introspection_sampler();
    set_global_catalog_introspection_sampler([] {
        CatalogIntrospectionSnapshot snapshot{};
        CatalogRelationSummary row{};
        row.database_name = "system";
        row.schema_name = "analytics";
        row.relation_name = "events";
        row.relation_kind = CatalogRelationKind::Table;
        snapshot.relations.push_back(row);
        return snapshot;
    });

    const auto snapshot = collect_global_catalog_introspection();
    REQUIRE(snapshot.relations.size() == 1U);
    CHECK(snapshot.relations.front().relation_name == "events");

    set_global_catalog_introspection_sampler(previous);
}
