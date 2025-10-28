#include "bored/planner/statistics_catalog.hpp"

#include <utility>

namespace bored::planner {

void TableStatistics::set_row_count(double value) noexcept
{
    row_count_ = value;
}

void TableStatistics::set_dead_row_count(double value) noexcept
{
    dead_row_count_ = value;
}

double TableStatistics::row_count() const noexcept
{
    return row_count_;
}

double TableStatistics::dead_row_count() const noexcept
{
    return dead_row_count_;
}

void TableStatistics::upsert_column(ColumnStatistics column)
{
    columns_[column.column_name] = std::move(column);
}

const ColumnStatistics* TableStatistics::find_column(const std::string& name) const noexcept
{
    const auto it = columns_.find(name);
    if (it == columns_.end()) {
        return nullptr;
    }
    return &it->second;
}

const std::unordered_map<std::string, ColumnStatistics>& TableStatistics::columns() const noexcept
{
    return columns_;
}

void StatisticsCatalog::register_table(RelationName relation, TableStatistics statistics)
{
    tables_[std::move(relation)] = std::move(statistics);
}

void StatisticsCatalog::upsert_column(RelationName relation, ColumnStatistics column)
{
    auto& table = tables_[relation];
    table.upsert_column(std::move(column));
}

const TableStatistics* StatisticsCatalog::find_table(const RelationName& relation) const noexcept
{
    const auto it = tables_.find(relation);
    if (it == tables_.end()) {
        return nullptr;
    }
    return &it->second;
}

TableStatistics* StatisticsCatalog::find_table(const RelationName& relation) noexcept
{
    const auto it = tables_.find(relation);
    if (it == tables_.end()) {
        return nullptr;
    }
    return &it->second;
}

const ColumnStatistics* StatisticsCatalog::find_column(const RelationName& relation,
                                                       const std::string& column) const noexcept
{
    const auto* table = find_table(relation);
    if (!table) {
        return nullptr;
    }
    return table->find_column(column);
}

void StatisticsCatalog::erase_table(const RelationName& relation) noexcept
{
    tables_.erase(relation);
}

void StatisticsCatalog::clear() noexcept
{
    tables_.clear();
}

std::size_t StatisticsCatalog::table_count() const noexcept
{
    return tables_.size();
}

}  // namespace bored::planner
