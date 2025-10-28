#include "bored/planner/statistics_catalog.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <optional>

using bored::planner::ColumnStatistics;
using bored::planner::StatisticsCatalog;
using bored::planner::TableStatistics;
using Catch::Approx;

namespace {

ColumnStatistics make_column(std::string name,
                             std::optional<double> distinct,
                             std::optional<double> null_fraction,
                             std::optional<double> average_width)
{
    ColumnStatistics column{};
    column.column_name = std::move(name);
    column.distinct_count = distinct;
    column.null_fraction = null_fraction;
    column.average_width = average_width;
    return column;
}

}  // namespace

TEST_CASE("Statistics catalog registers and retrieves table statistics")
{
    StatisticsCatalog catalog;

    TableStatistics table_stats;
    table_stats.set_row_count(1024.0);
    table_stats.set_dead_row_count(24.0);
    table_stats.upsert_column(make_column("id", 1024.0, 0.0, 8.0));
    table_stats.upsert_column(make_column("name", 1000.0, 0.05, 42.0));

    catalog.register_table("public.accounts", table_stats);

    const auto* retrieved = catalog.find_table("public.accounts");
    REQUIRE(retrieved);
    CHECK(retrieved->row_count() == Approx(1024.0));
    CHECK(retrieved->dead_row_count() == Approx(24.0));
    REQUIRE(retrieved->columns().size() == 2U);

    const auto* id_stats = retrieved->find_column("id");
    REQUIRE(id_stats);
    CHECK(id_stats->distinct_count.has_value());
    CHECK(id_stats->distinct_count.value() == Approx(1024.0));
    CHECK(id_stats->null_fraction.value_or(-1.0) == Approx(0.0));
    CHECK(id_stats->average_width.value_or(-1.0) == Approx(8.0));

    const auto* name_stats = catalog.find_column("public.accounts", "name");
    REQUIRE(name_stats);
    CHECK(name_stats->distinct_count.value_or(0.0) == Approx(1000.0));
    CHECK(name_stats->null_fraction.value_or(0.0) == Approx(0.05));
    CHECK(name_stats->average_width.value_or(0.0) == Approx(42.0));
}

TEST_CASE("Statistics catalog upserts columns and clears data")
{
    StatisticsCatalog catalog;

    catalog.upsert_column("public.orders", make_column("order_id", 10000.0, 0.0, 8.0));

    auto* table = catalog.find_table("public.orders");
    REQUIRE(table);
    CHECK(table->row_count() == Approx(0.0));
    CHECK(table->dead_row_count() == Approx(0.0));
    REQUIRE(table->columns().size() == 1U);

    catalog.upsert_column("public.orders", make_column("customer_id", 2500.0, 0.0, 16.0));

    const auto* customer = catalog.find_column("public.orders", "customer_id");
    REQUIRE(customer);
    CHECK(customer->distinct_count.value_or(0.0) == Approx(2500.0));

    catalog.erase_table("public.orders");
    CHECK(catalog.find_table("public.orders") == nullptr);
    CHECK(catalog.table_count() == 0U);

    TableStatistics another;
    another.set_row_count(5.0);
    catalog.register_table("public.small", another);
    CHECK(catalog.table_count() == 1U);
    catalog.clear();
    CHECK(catalog.table_count() == 0U);
}
