#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"
#include "bored/parser/relational/logical_lowering.hpp"
#include "bored/parser/relational/logical_plan_dump.hpp"
#include "bored/parser/relational/logical_plan_printer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cctype>
#include <memory>
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

relational::TableMetadata make_hierarchy_metadata()
{
    relational::TableMetadata metadata{};
    metadata.database_id = catalog::DatabaseId{1U};
    metadata.schema_id = catalog::SchemaId{12U};
    metadata.relation_id = catalog::RelationId{300U};
    metadata.schema_name = "sales";
    metadata.table_name = "hierarchy";
    metadata.columns.push_back(relational::ColumnMetadata{catalog::ColumnId{1U}, catalog::CatalogColumnType::Int64, "id"});
    metadata.columns.push_back(relational::ColumnMetadata{catalog::ColumnId{2U}, catalog::CatalogColumnType::Int64, "manager_id"});
    return metadata;
}

struct PlanFixture final {
    std::shared_ptr<bored::parser::SelectParseResult> parse;
    relational::LogicalOperatorPtr plan;
};

PlanFixture build_plan(std::string_view sql)
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    auto parse_result = std::make_shared<bored::parser::SelectParseResult>(bored::parser::parse_select(std::string(sql)));
    REQUIRE(parse_result->success());
    REQUIRE(parse_result->statement != nullptr);

    relational::BinderConfig config{};
    config.catalog = &catalog_adapter;
    config.default_schema = std::string{"sales"};

    auto binding = relational::bind_select(config, *parse_result->statement);
    REQUIRE(binding.success());

    auto lowering = relational::lower_select(*parse_result->statement);
    REQUIRE(lowering.success());
    PlanFixture fixture{};
    fixture.parse = std::move(parse_result);
    fixture.plan = std::move(lowering.plan);
    return fixture;
}

}  // namespace

TEST_CASE("plan printer renders linear pipeline", "[parser][logical_plan_printer]")
{
    const std::string sql =
        "SELECT inv.quantity AS qty FROM sales.inventory AS inv WHERE inv.quantity > 10 ORDER BY qty LIMIT 5;";

    auto fixture = build_plan(sql);
    REQUIRE(fixture.plan != nullptr);

    const auto text = relational::describe_plan(*fixture.plan);
    const std::string expected =
        "Limit row_count=5\n"
    "  Sort keys=[qty ASC]\n"
        "    Project columns=[qty:UINT32?]\n"
        "      Filter predicate=(inv.quantity > 10)\n"
        "        Scan table=sales.inventory alias=inv\n";

    CHECK(text == expected);
}

TEST_CASE("plan printer handles bare projections", "[parser][logical_plan_printer]")
{
    const std::string sql =
        "SELECT inv.id, inv.name FROM sales.inventory AS inv;";

    auto fixture = build_plan(sql);
    REQUIRE(fixture.plan != nullptr);

    const auto text = relational::describe_plan(*fixture.plan);
    CHECK(text.find("Project columns=[inv.id:INT64?", 0) != std::string::npos);
    CHECK(text.find("Scan table=sales.inventory", 0) != std::string::npos);
}

TEST_CASE("lower_select invokes plan sink when configured", "[parser][logical_plan_printer]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    const std::string sql =
        "SELECT inv.quantity AS qty FROM sales.inventory AS inv WHERE inv.quantity > 10 ORDER BY qty LIMIT 5;";

    auto parse_result = bored::parser::parse_select(sql);
    if (!parse_result.success()) {
        for (const auto& diagnostic : parse_result.diagnostics) {
            INFO("parse diagnostic: " << diagnostic.message);
        }
    }
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    relational::BinderConfig binder_config{};
    binder_config.catalog = &catalog_adapter;
    binder_config.default_schema = std::string{"sales"};

    auto binding = relational::bind_select(binder_config, *parse_result.statement);
    REQUIRE(binding.success());

    bool sink_invoked = false;
    relational::LoweringConfig lowering_config{};
    lowering_config.plan_sink = [&](const relational::LogicalOperator& plan) {
        sink_invoked = true;
        const auto text = relational::describe_plan(plan);
        CHECK(text.find("Limit row_count=5") != std::string::npos);
    };

    auto lowering = relational::lower_select(*parse_result.statement, lowering_config);
    if (!lowering.diagnostics.empty()) {
        CAPTURE(lowering.diagnostics.front().message);
    }
    REQUIRE(lowering.success());
    CHECK(sink_invoked);
}

TEST_CASE("plan dump utility produces plan text", "[parser][logical_plan_printer]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    relational::LogicalPlanDumpOptions options{};
    options.binder_config.catalog = &catalog_adapter;
    options.binder_config.default_schema = std::string{"sales"};
    options.include_normalization = true;

    bool sink_invoked = false;
    std::string sink_text;
    options.plan_text_sink = [&](std::string_view text) {
        sink_invoked = true;
        sink_text.assign(text.begin(), text.end());
    };

    const std::string sql =
        "SELECT inv.quantity AS qty FROM sales.inventory AS inv WHERE inv.quantity > 10 ORDER BY qty LIMIT 5;";

    const auto dump = relational::dump_select_plan(sql, options);
    REQUIRE(dump.success());
    CAPTURE(dump.plan_text);
    CHECK(dump.plan_text.find("Sort keys=[") != std::string::npos);
    CHECK(dump.plan_text.find("Scan table=sales.inventory alias=inv") != std::string::npos);
    CHECK(sink_invoked);
    CHECK(sink_text == dump.plan_text);
    REQUIRE(dump.normalization.has_value());
    CHECK_FALSE(dump.normalization->filters.empty());

    bool overload_sink_invoked = false;
    const auto dumped_again = relational::dump_select_plan(sql,
                                                           options,
                                                           [&](std::string_view text) {
                                                               overload_sink_invoked = true;
                                                               CHECK(text.find("Limit row_count=5") != std::string::npos);
                                                           });
    REQUIRE(dumped_again.success());
    CHECK(overload_sink_invoked);
}

TEST_CASE("plan printer renders join pipeline", "[parser][logical_plan_printer]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());
    catalog_adapter.add_table(make_shipments_metadata());

    const std::string sql =
        "SELECT inv.id, shp.status FROM sales.inventory AS inv INNER JOIN sales.shipments AS shp ON inv.id = shp.id;";

    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    relational::BinderConfig binder_config{};
    binder_config.catalog = &catalog_adapter;
    binder_config.default_schema = std::string{"sales"};

    auto binding = relational::bind_select(binder_config, *parse_result.statement);
    REQUIRE(binding.success());

    auto lowering = relational::lower_select(*parse_result.statement);
    REQUIRE(lowering.success());
    REQUIRE(lowering.plan != nullptr);

    const auto text = relational::describe_plan(*lowering.plan);
    const std::string expected =
        "Project columns=[inv.id:INT64?, shp.status:UTF8?]\n"
        "  Join type=Inner predicate=(inv.id = shp.id)\n"
        "    Scan table=sales.inventory alias=inv\n"
        "    Scan table=sales.shipments alias=shp\n";

    CHECK(text == expected);
}

TEST_CASE("plan printer renders recursive CTE", "[parser][logical_plan_printer]")
{
    StubCatalog catalog_adapter;
    catalog_adapter.add_table(make_inventory_metadata());

    const std::string sql =
        "WITH RECURSIVE chain(id) AS (\n"
        " SELECT inventory.id FROM sales.inventory AS inventory\n"
        " UNION ALL\n"
        " SELECT chain.id FROM chain\n"
        ") SELECT chain.id FROM chain;";

    auto parse_result = bored::parser::parse_select(sql);
    REQUIRE(parse_result.success());
    REQUIRE(parse_result.statement != nullptr);

    relational::BinderConfig binder_config{};
    binder_config.catalog = &catalog_adapter;
    binder_config.default_schema = std::string{"sales"};

    auto binding = relational::bind_select(binder_config, *parse_result.statement);
    REQUIRE(binding.success());

    auto lowering = relational::lower_select(*parse_result.statement);
    REQUIRE(lowering.success());
    REQUIRE(lowering.plan != nullptr);

    const auto text = relational::describe_plan(*lowering.plan);
    CHECK(text.find("RecursiveCTE name=chain") != std::string::npos);
    CHECK(text.find("Anchor:") != std::string::npos);
    CHECK(text.find("Recursive:") != std::string::npos);
    CHECK(text.find("CteScan source=chain") != std::string::npos);
    CHECK(text.find("recursive=true") != std::string::npos);
}
