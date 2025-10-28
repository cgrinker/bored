#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>

namespace bored::planner {

struct ColumnStatistics final {
    std::string column_name;
    std::optional<double> distinct_count;
    std::optional<double> null_fraction;
    std::optional<double> average_width;
};

class TableStatistics final {
public:
    void set_row_count(double value) noexcept;
    void set_dead_row_count(double value) noexcept;

    [[nodiscard]] double row_count() const noexcept;
    [[nodiscard]] double dead_row_count() const noexcept;

    void upsert_column(ColumnStatistics column);

    [[nodiscard]] const ColumnStatistics* find_column(const std::string& name) const noexcept;

    [[nodiscard]] const std::unordered_map<std::string, ColumnStatistics>& columns() const noexcept;

private:
    double row_count_ = 0.0;
    double dead_row_count_ = 0.0;
    std::unordered_map<std::string, ColumnStatistics> columns_{};
};

class StatisticsCatalog final {
public:
    using RelationName = std::string;

    void register_table(RelationName relation, TableStatistics statistics);
    void upsert_column(RelationName relation, ColumnStatistics column);

    [[nodiscard]] const TableStatistics* find_table(const RelationName& relation) const noexcept;
    [[nodiscard]] TableStatistics* find_table(const RelationName& relation) noexcept;

    [[nodiscard]] const ColumnStatistics* find_column(const RelationName& relation,
                                                      const std::string& column) const noexcept;

    void erase_table(const RelationName& relation) noexcept;
    void clear() noexcept;

    [[nodiscard]] std::size_t table_count() const noexcept;

private:
    std::unordered_map<RelationName, TableStatistics> tables_{};
};

}  // namespace bored::planner
