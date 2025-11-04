#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cctype>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace relational = bored::parser::relational;
namespace catalog = bored::catalog;

namespace {

std::string normalise(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (char ch : text) {
        result.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return result;
}

class StubCatalog final : public relational::BinderCatalog {
public:
    void add_table(relational::TableMetadata metadata)
    {
        tables_.push_back(std::move(metadata));
    }

    std::optional<relational::TableMetadata> lookup_table(std::optional<std::string_view> schema,
                                                          std::string_view table) const override
    {
        const auto table_key = normalise(table);
        const auto schema_key = schema ? std::optional<std::string>(normalise(*schema)) : std::nullopt;

        std::optional<relational::TableMetadata> result{};
        for (const auto& entry : tables_) {
            if (normalise(entry.table_name) != table_key) {
                continue;
            }
            if (schema_key && normalise(entry.schema_name) != *schema_key) {
                continue;
            }
            if (result.has_value()) {
                // Ambiguous match without schema qualification.
                return std::nullopt;
            }
            result = entry;
        }
        return result;
    }

private:
    std::vector<relational::TableMetadata> tables_{};
};

relational::TableMetadata make_inventory_metadata()
{
    relational::TableMetadata metadata{};
    metadata.database_id = catalog::DatabaseId{1U};
    metadata.schema_id = catalog::SchemaId{10U};
    metadata.relation_id = catalog::RelationId{100U};
    metadata.schema_name = "sales";
    metadata.table_name = "inventory";
    metadata.columns.push_back(relational::ColumnMetadata{catalog::ColumnId{1U}, catalog::CatalogColumnType::Int64, "id"});
    metadata.columns.push_back(relational::ColumnMetadata{catalog::ColumnId{2U}, catalog::CatalogColumnType::Utf8, "name"});
    metadata.columns.push_back(relational::ColumnMetadata{catalog::ColumnId{3U}, catalog::CatalogColumnType::UInt32, "quantity"});
    return metadata;
}

relational::TableMetadata make_shipments_metadata()
{
    relational::TableMetadata metadata{};
    metadata.database_id = catalog::DatabaseId{1U};
    metadata.schema_id = catalog::SchemaId{11U};
    metadata.relation_id = catalog::RelationId{200U};
    metadata.schema_name = "sales";
    metadata.table_name = "shipments";
    metadata.columns.push_back(relational::ColumnMetadata{catalog::ColumnId{1U}, catalog::CatalogColumnType::Int64, "id"});
    metadata.columns.push_back(relational::ColumnMetadata{catalog::ColumnId{2U}, catalog::CatalogColumnType::UInt32, "quantity"});
    metadata.columns.push_back(relational::ColumnMetadata{catalog::ColumnId{3U}, catalog::CatalogColumnType::Utf8, "status"});
    return metadata;
}

}  // namespace

TEST_CASE("binder resolves qualified columns", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.id, inv.name FROM sales.inventory AS inv WHERE inv.id = 42;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding_result.diagnostics.empty());
    REQUIRE(binding_result.success());

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->from != nullptr);
    REQUIRE(query->from->binding.has_value());
    CHECK(query->from->binding->table_alias.has_value());
    CHECK(*query->from->binding->table_alias == "inv");
    CHECK(query->from->binding->schema_name == "sales");
    CHECK(query->from->binding->table_name == "inventory");

    REQUIRE(query->select_items.size() == 2U);
    auto* first_item = query->select_items.front();
    REQUIRE(first_item != nullptr);
    auto& first_identifier = static_cast<relational::IdentifierExpression&>(*first_item->expression);
    REQUIRE(first_identifier.binding.has_value());
    CHECK(first_identifier.binding->column_name == "id");
    CHECK(first_identifier.binding->table_alias.has_value());
    CHECK(*first_identifier.binding->table_alias == "inv");

    REQUIRE(query->where != nullptr);
    auto& predicate = static_cast<relational::BinaryExpression&>(*query->where);
    REQUIRE(predicate.left != nullptr);
    auto& left_identifier = static_cast<relational::IdentifierExpression&>(*predicate.left);
    REQUIRE(left_identifier.binding.has_value());
    CHECK(left_identifier.binding->column_name == "id");
}

TEST_CASE("binder rejects WITH clauses", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "WITH items AS (SELECT inventory.id FROM sales.inventory AS inventory) SELECT * FROM items;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message ==
          "Common table expressions (WITH clauses) are not supported yet");
}

TEST_CASE("binder rejects missing columns", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.serial FROM sales.inventory AS inv;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message == "Column 'serial' not found on table 'inv'");
}

TEST_CASE("binder resolves qualified star expressions", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.* FROM sales.inventory AS inv;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding_result.success());

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->select_items.size() == 1U);
    auto* item = query->select_items.front();
    REQUIRE(item != nullptr);
    auto& star = static_cast<relational::StarExpression&>(*item->expression);
    REQUIRE(star.binding.has_value());
    CHECK(star.binding->table_alias.has_value());
    CHECK(*star.binding->table_alias == "inv");
}

TEST_CASE("binder binds join predicates", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());
    catalog_adapter.add_table(make_shipments_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.id, shp.status FROM sales.inventory AS inv INNER JOIN sales.shipments AS shp ON inv.id = shp.id;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding_result.success());

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->from_tables.size() == 2U);

    auto* left_table = query->from_tables.front();
    REQUIRE(left_table != nullptr);
    REQUIRE(left_table->binding.has_value());
    CHECK(left_table->binding->relation_id == catalog::RelationId{100U});

    auto* right_table = query->from_tables.back();
    REQUIRE(right_table != nullptr);
    REQUIRE(right_table->binding.has_value());
    CHECK(right_table->binding->relation_id == catalog::RelationId{200U});

    REQUIRE(query->joins.size() == 1U);
    const auto& join = query->joins.front();
    REQUIRE(join.predicate != nullptr);
    REQUIRE(join.predicate->kind == relational::NodeKind::BinaryExpression);
    const auto& predicate = static_cast<const relational::BinaryExpression&>(*join.predicate);
    REQUIRE(predicate.left != nullptr);
    REQUIRE(predicate.left->kind == relational::NodeKind::IdentifierExpression);
    const auto& left_id = static_cast<const relational::IdentifierExpression&>(*predicate.left);
    REQUIRE(left_id.binding.has_value());
    CHECK(left_id.binding->relation_id == catalog::RelationId{100U});

    REQUIRE(predicate.right != nullptr);
    REQUIRE(predicate.right->kind == relational::NodeKind::IdentifierExpression);
    const auto& right_id = static_cast<const relational::IdentifierExpression&>(*predicate.right);
    REQUIRE(right_id.binding.has_value());
    CHECK(right_id.binding->relation_id == catalog::RelationId{200U});
}

TEST_CASE("binder reports unknown star qualifier", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT unknown_alias.* FROM sales.inventory AS inv;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message == "Table alias 'unknown_alias' not found for star expression");
}

TEST_CASE("binder annotates expression types", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.quantity, 5 FROM sales.inventory AS inv WHERE inv.quantity = 10;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding_result.success());

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->select_items.size() == 2U);

    auto* quantity_item = query->select_items.front();
    REQUIRE(quantity_item != nullptr);
    auto& quantity_expr = static_cast<relational::IdentifierExpression&>(*quantity_item->expression);
    REQUIRE(quantity_expr.inferred_type.has_value());
    CHECK(quantity_expr.inferred_type->type == relational::ScalarType::UInt32);
    CHECK(quantity_expr.inferred_type->nullable);

    auto* literal_item = query->select_items.back();
    REQUIRE(literal_item != nullptr);
    auto& literal_expr = static_cast<relational::LiteralExpression&>(*literal_item->expression);
    REQUIRE(literal_expr.inferred_type.has_value());
    CHECK(literal_expr.inferred_type->type == relational::ScalarType::Int64);
    CHECK_FALSE(literal_expr.inferred_type->nullable);

    REQUIRE(query->where != nullptr);
    auto& predicate = static_cast<relational::BinaryExpression&>(*query->where);
    REQUIRE(predicate.inferred_type.has_value());
    CHECK(predicate.inferred_type->type == relational::ScalarType::Boolean);
    CHECK(predicate.inferred_type->nullable);

    auto& predicate_left = static_cast<relational::IdentifierExpression&>(*predicate.left);
    REQUIRE(predicate_left.required_coercion.has_value());
    CHECK(predicate_left.required_coercion->target_type == relational::ScalarType::Int64);
    auto& predicate_right = static_cast<relational::LiteralExpression&>(*predicate.right);
    CHECK_FALSE(predicate_right.required_coercion.has_value());
}

TEST_CASE("binder reports type mismatches", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.name FROM sales.inventory AS inv WHERE inv.name = 10;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message == "Type mismatch: cannot compare UTF8 to INT64");

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->where != nullptr);
    auto& predicate = static_cast<relational::BinaryExpression&>(*query->where);
    auto& left_identifier = static_cast<relational::IdentifierExpression&>(*predicate.left);
    CHECK_FALSE(left_identifier.required_coercion.has_value());
}

TEST_CASE("binder records decimal promotions", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.quantity FROM sales.inventory AS inv WHERE inv.quantity = 10.5;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding_result.success());

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->where != nullptr);

    auto& predicate = static_cast<relational::BinaryExpression&>(*query->where);
    auto& left_identifier = static_cast<relational::IdentifierExpression&>(*predicate.left);
    REQUIRE(left_identifier.required_coercion.has_value());
    CHECK(left_identifier.required_coercion->target_type == relational::ScalarType::Decimal);

    auto& right_literal = static_cast<relational::LiteralExpression&>(*predicate.right);
    CHECK_FALSE(right_literal.required_coercion.has_value());
}

TEST_CASE("binder resolves order by aliases", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.quantity AS qty FROM sales.inventory AS inv ORDER BY qty;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding_result.success());

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->order_by.size() == 1U);
    auto* item = query->order_by.front();
    REQUIRE(item != nullptr);
    auto& identifier = static_cast<relational::IdentifierExpression&>(*item->expression);
    CHECK_FALSE(identifier.binding.has_value());
    REQUIRE(identifier.inferred_type.has_value());
    CHECK(identifier.inferred_type->type == relational::ScalarType::UInt32);
}

TEST_CASE("binder rejects duplicate select aliases", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.id AS key, inv.quantity AS key FROM sales.inventory AS inv ORDER BY key;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message == "Select item alias 'key' is ambiguous");
}

TEST_CASE("binder resolves group by columns", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.quantity FROM sales.inventory AS inv GROUP BY inv.quantity;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding_result.success());

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->group_by.size() == 1U);
    auto* expression = query->group_by.front();
    REQUIRE(expression != nullptr);
    auto& identifier = static_cast<relational::IdentifierExpression&>(*expression);
    REQUIRE(identifier.binding.has_value());
    CHECK(identifier.binding->column_name == "quantity");
}

TEST_CASE("binder resolves group by aliases", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.quantity AS qty FROM sales.inventory AS inv GROUP BY qty;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding_result.success());

    auto* query = parse_result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->group_by.size() == 1U);
    auto* expression = query->group_by.front();
    REQUIRE(expression != nullptr);
    auto& identifier = static_cast<relational::IdentifierExpression&>(*expression);
    CHECK_FALSE(identifier.binding.has_value());
    REQUIRE(identifier.inferred_type.has_value());
    CHECK(identifier.inferred_type->type == relational::ScalarType::UInt32);
}

TEST_CASE("binder reports unknown group by columns", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.quantity FROM sales.inventory AS inv GROUP BY inv.serial;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message == "Column 'serial' not found on table 'inv'");
}

TEST_CASE("binder reports ambiguous unqualified column references", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());
    catalog_adapter.add_table(make_shipments_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT id FROM sales.inventory AS inv, sales.shipments AS sh;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message == "Column reference 'id' is ambiguous");
}

TEST_CASE("binder reports ambiguous qualified column references", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.id FROM sales.inventory AS inv, sales.inventory AS inv;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message == "Column reference 'inv.id' is ambiguous");
}

TEST_CASE("binder reports unknown qualified identifiers", "[parser][binder]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());
    catalog_adapter.add_table(make_shipments_metadata());

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    const std::string sql =
        "SELECT inv.id FROM sales.inventory AS inv, sales.shipments AS sh WHERE missing.id = 1;";
    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    auto binding_result = relational::bind_select(config, *parse_result.statement);
    REQUIRE_FALSE(binding_result.success());
    REQUIRE(binding_result.diagnostics.size() == 1U);
    CHECK(binding_result.diagnostics.front().message == "Column 'id' not found on table 'missing'");
}
