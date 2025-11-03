#include "bored/catalog/catalog_encoding.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace bored::catalog;

TEST_CASE("Catalog constraint encoding round trips metadata")
{
    CatalogConstraintDescriptor descriptor{};
    descriptor.tuple = CatalogTupleDescriptor{.xmin = 11U, .xmax = 19U, .visibility_flags = 0xAA55U, .reserved = 0U};
    descriptor.constraint_id = ConstraintId{42U};
    descriptor.relation_id = RelationId{77U};
    descriptor.constraint_type = CatalogConstraintType::PrimaryKey;
    descriptor.backing_index_id = IndexId{88U};
    descriptor.referenced_relation_id = RelationId{109U};
    descriptor.key_columns = "id";
    descriptor.referenced_columns = "ref_id";
    descriptor.name = "pk_items";

    const auto expected_size = catalog_constraint_tuple_size(descriptor.name, descriptor.key_columns, descriptor.referenced_columns);
    const auto buffer = serialize_catalog_constraint(descriptor);
    REQUIRE(buffer.size() == expected_size);

    const auto view = decode_catalog_constraint(buffer);
    REQUIRE(view.has_value());
    CHECK(view->tuple.xmin == descriptor.tuple.xmin);
    CHECK(view->tuple.xmax == descriptor.tuple.xmax);
    CHECK(view->tuple.visibility_flags == descriptor.tuple.visibility_flags);
    CHECK(view->constraint_id == descriptor.constraint_id);
    CHECK(view->relation_id == descriptor.relation_id);
    CHECK(view->constraint_type == descriptor.constraint_type);
    CHECK(view->backing_index_id == descriptor.backing_index_id);
    CHECK(view->referenced_relation_id == descriptor.referenced_relation_id);
    CHECK(view->name == descriptor.name);
    CHECK(view->key_columns == descriptor.key_columns);
    CHECK(view->referenced_columns == descriptor.referenced_columns);
}

TEST_CASE("Catalog sequence encoding round trips metadata")
{
    CatalogSequenceDescriptor descriptor{};
    descriptor.tuple = CatalogTupleDescriptor{.xmin = 21U, .xmax = 31U, .visibility_flags = 0x55AAU, .reserved = 0U};
    descriptor.sequence_id = SequenceId{305U};
    descriptor.schema_id = SchemaId{701U};
    descriptor.owning_relation_id = RelationId{901U};
    descriptor.owning_column_id = ColumnId{17U};
    descriptor.start_value = 100U;
    descriptor.next_value = 115U;
    descriptor.increment = -5;
    descriptor.min_value = 1U;
    descriptor.max_value = 10'000U;
    descriptor.cache_size = 32U;
    descriptor.cycle = true;
    descriptor.name = "items_seq";

    const auto expected_size = catalog_sequence_tuple_size(descriptor.name);
    const auto buffer = serialize_catalog_sequence(descriptor);
    REQUIRE(buffer.size() == expected_size);

    const auto view = decode_catalog_sequence(buffer);
    REQUIRE(view.has_value());
    CHECK(view->tuple.xmin == descriptor.tuple.xmin);
    CHECK(view->tuple.xmax == descriptor.tuple.xmax);
    CHECK(view->tuple.visibility_flags == descriptor.tuple.visibility_flags);
    CHECK(view->sequence_id == descriptor.sequence_id);
    CHECK(view->schema_id == descriptor.schema_id);
    CHECK(view->owning_relation_id == descriptor.owning_relation_id);
    CHECK(view->owning_column_id == descriptor.owning_column_id);
    CHECK(view->start_value == descriptor.start_value);
    CHECK(view->next_value == descriptor.next_value);
    CHECK(view->increment == descriptor.increment);
    CHECK(view->min_value == descriptor.min_value);
    CHECK(view->max_value == descriptor.max_value);
    CHECK(view->cache_size == descriptor.cache_size);
    CHECK(view->cycle == descriptor.cycle);
    CHECK(view->name == descriptor.name);
}