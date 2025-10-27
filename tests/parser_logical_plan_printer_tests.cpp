#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"
#include "bored/parser/relational/logical_lowering.hpp"
#include "bored/parser/relational/logical_plan_printer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cctype>
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

relational::LogicalOperatorPtr build_plan(std::string_view sql)
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    auto parse_result = bored::parser::parse_select(std::string(sql));
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    auto binding = relational::bind_select(config, *parse_result.statement);
    REQUIRE(binding.success());

    auto lowering = relational::lower_select(*parse_result.statement);
    REQUIRE(lowering.success());
    return std::move(lowering.plan);
}

}  // namespace

TEST_CASE("plan printer renders linear pipeline", "[parser][logical_plan_printer]")
{
    const std::string sql =
        "SELECT inv.quantity AS qty FROM sales.inventory AS inv WHERE inv.quantity > 10 ORDER BY qty LIMIT 5;";

    auto plan = build_plan(sql);
    REQUIRE(plan != nullptr);

    const auto text = relational::describe_plan(*plan);
    const std::string expected =
        "Limit row_count=5\n"
        "  Sort keys=[<identifier> ASC]\n"
        "    Project columns=[qty:UINT32?]\n"
        "      Filter predicate=(inv.quantity > 10)\n"
        "        Scan table=sales.inventory alias=inv\n";

    CHECK(text == expected);
}

TEST_CASE("plan printer handles bare projections", "[parser][logical_plan_printer]")
{
    const std::string sql =
        "SELECT inv.id, inv.name FROM sales.inventory AS inv;";

    auto plan = build_plan(sql);
    REQUIRE(plan != nullptr);

    const auto text = relational::describe_plan(*plan);
    CHECK(text.find("Project columns=[inv.id:INT64?", 0) != std::string::npos);
    CHECK(text.find("Scan table=sales.inventory", 0) != std::string::npos);
}
