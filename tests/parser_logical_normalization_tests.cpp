#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"
#include "bored/parser/relational/logical_lowering.hpp"
#include "bored/parser/relational/logical_normalization.hpp"

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

struct LoweringFixture final {
    relational::LoweringResult lowering{};
    relational::LogicalOperatorPtr& plan;
    std::vector<bored::parser::ParserDiagnostic>& diagnostics;
    bored::parser::SelectParseResult parse{};

    LoweringFixture() : plan(lowering.plan), diagnostics(lowering.diagnostics) {}

    [[nodiscard]] bool success() const noexcept { return lowering.success(); }
};

LoweringFixture lower_sql(std::string_view sql, StubCatalog& catalog_adapter)
{
    LoweringFixture fixture{};
    fixture.parse = bored::parser::parse_select(std::string(sql));
    REQUIRE(fixture.parse.success());
    REQUIRE(fixture.parse.statement != nullptr);

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    auto binding = relational::bind_select(config, *fixture.parse.statement);
    REQUIRE(binding.success());

    auto lowering = relational::lower_select(*fixture.parse.statement);
    fixture.plan = std::move(lowering.plan);
    fixture.diagnostics = std::move(lowering.diagnostics);
    return fixture;
}

}  // namespace

TEST_CASE("normalization extracts filter predicates", "[parser][logical_normalization]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    const std::string sql =
        "SELECT inv.quantity FROM sales.inventory AS inv WHERE inv.quantity > 10 ORDER BY inv.quantity;";

    auto lowering = lower_sql(sql, catalog_adapter);
    REQUIRE(lowering.success());
    REQUIRE(lowering.plan != nullptr);

    auto result = relational::normalize_plan(*lowering.plan);
    REQUIRE(result.filters.size() == 1U);
    auto& filter = result.filters.front();
    REQUIRE(filter.node != nullptr);
    REQUIRE(filter.predicates.size() == 1U);
    CHECK(filter.predicates.front() != nullptr);
}

TEST_CASE("normalization preserves projection order", "[parser][logical_normalization]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    const std::string sql =
        "SELECT inv.id AS product_id, inv.name FROM sales.inventory AS inv WHERE inv.id > 10;";

    auto lowering = lower_sql(sql, catalog_adapter);
    REQUIRE(lowering.success());
    REQUIRE(lowering.plan != nullptr);

    auto result = relational::normalize_plan(*lowering.plan);
    REQUIRE(result.projections.size() == 1U);
    const auto& projection = result.projections.front();
    REQUIRE(projection.projections.size() == 2U);
    CHECK(projection.projections.front()->alias.has_value());
    CHECK(*projection.projections.front()->alias == "product_id");
}

TEST_CASE("normalization captures join criteria", "[parser][logical_normalization]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());
    catalog_adapter.add_table(make_shipments_metadata());

    const std::string sql =
        "SELECT inv.id FROM sales.inventory AS inv INNER JOIN sales.shipments AS shp ON inv.id = shp.id;";

    auto lowering = lower_sql(sql, catalog_adapter);
    REQUIRE(lowering.success());
    REQUIRE(lowering.plan != nullptr);

    auto result = relational::normalize_plan(*lowering.plan);
    REQUIRE(result.joins.size() == 1U);
    const auto& join = result.joins.front();
    REQUIRE(join.node != nullptr);
    CHECK(join.join_type == relational::JoinType::Inner);
    REQUIRE(join.predicate != nullptr);
    REQUIRE(join.equi_conditions.size() == 1U);
    CHECK(join.equi_conditions.front() == join.predicate);
}
