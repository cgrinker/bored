#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"
#include "bored/parser/relational/logical_lowering.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

namespace relational = bored::parser::relational;
namespace catalog = bored::catalog;

namespace {

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
                return std::nullopt;
            }
            result = entry;
        }
        return result;
    }

private:
    static std::string normalise(std::string_view text)
    {
        std::string result;
        result.reserve(text.size());
        for (char ch : text) {
            result.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        }
        return result;
    }

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

relational::LoweringResult lower_sql(std::string_view sql, StubCatalog& catalog_adapter)
{
    auto parse_result = bored::parser::parse_select(std::string(sql));
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    auto binding = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding.success());

    return relational::lower_select(*parse_result.statement);
}

}  // namespace

TEST_CASE("lowering builds linear select pipeline", "[parser][logical_lowering]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    const std::string sql =
        "SELECT inv.quantity AS qty FROM sales.inventory AS inv WHERE inv.quantity > 10 ORDER BY qty LIMIT 5;";

    auto lowering = lower_sql(sql, catalog_adapter);
    REQUIRE(lowering.success());
    REQUIRE(lowering.plan != nullptr);

    auto* limit = static_cast<relational::LogicalLimit*>(lowering.plan.get());
    REQUIRE(limit->input != nullptr);
    CHECK(limit->row_count != nullptr);

    auto* sort = static_cast<relational::LogicalSort*>(limit->input.get());
    REQUIRE(sort->input != nullptr);
    REQUIRE(sort->keys.size() == 1U);

    auto* project = static_cast<relational::LogicalProject*>(sort->input.get());
    REQUIRE(project->input != nullptr);
    REQUIRE(project->output_schema.size() == 1U);
    CHECK(project->output_schema.front().name == "qty");
    CHECK(project->output_schema.front().type == relational::ScalarType::UInt32);

    auto* filter = static_cast<relational::LogicalFilter*>(project->input.get());
    REQUIRE(filter->input != nullptr);
    CHECK(filter->predicate != nullptr);

    auto* scan = static_cast<relational::LogicalScan*>(filter->input.get());
    CHECK(scan->table.table_name == "inventory");
}

TEST_CASE("lowering reports unsupported scenarios", "[parser][logical_lowering]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    const std::string sql =
        "SELECT inv.quantity FROM sales.inventory AS inv GROUP BY inv.quantity;";

    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    auto binding = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding.success());

    auto lowering = relational::lower_select(*parse_result.statement);
    REQUIRE_FALSE(lowering.success());
    REQUIRE(lowering.diagnostics.size() == 1U);
    CHECK(lowering.diagnostics.front().message == "GROUP BY lowering is not implemented");
}
