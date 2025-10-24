#pragma once

#include <cstdint>

namespace bored::catalog {

struct DatabaseId final {
    std::uint64_t value = 0U;
    [[nodiscard]] constexpr bool is_valid() const noexcept { return value != 0U; }
};

struct SchemaId final {
    std::uint64_t value = 0U;
    [[nodiscard]] constexpr bool is_valid() const noexcept { return value != 0U; }
};

struct RelationId final {
    std::uint64_t value = 0U;
    [[nodiscard]] constexpr bool is_valid() const noexcept { return value != 0U; }
};

struct ColumnId final {
    std::uint64_t value = 0U;
    [[nodiscard]] constexpr bool is_valid() const noexcept { return value != 0U; }
};

struct IndexId final {
    std::uint64_t value = 0U;
    [[nodiscard]] constexpr bool is_valid() const noexcept { return value != 0U; }
};

constexpr bool operator==(DatabaseId lhs, DatabaseId rhs) noexcept { return lhs.value == rhs.value; }
constexpr bool operator!=(DatabaseId lhs, DatabaseId rhs) noexcept { return !(lhs == rhs); }
constexpr bool operator==(SchemaId lhs, SchemaId rhs) noexcept { return lhs.value == rhs.value; }
constexpr bool operator!=(SchemaId lhs, SchemaId rhs) noexcept { return !(lhs == rhs); }
constexpr bool operator==(RelationId lhs, RelationId rhs) noexcept { return lhs.value == rhs.value; }
constexpr bool operator!=(RelationId lhs, RelationId rhs) noexcept { return !(lhs == rhs); }
constexpr bool operator==(ColumnId lhs, ColumnId rhs) noexcept { return lhs.value == rhs.value; }
constexpr bool operator!=(ColumnId lhs, ColumnId rhs) noexcept { return !(lhs == rhs); }
constexpr bool operator==(IndexId lhs, IndexId rhs) noexcept { return lhs.value == rhs.value; }
constexpr bool operator!=(IndexId lhs, IndexId rhs) noexcept { return !(lhs == rhs); }

}  // namespace bored::catalog
