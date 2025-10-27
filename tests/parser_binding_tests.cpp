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
