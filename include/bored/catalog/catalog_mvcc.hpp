#pragma once

#include "bored/catalog/catalog_relations.hpp"
#include "bored/txn/transaction_types.hpp"

#include <algorithm>
#include <cstdint>
#include <vector>

namespace bored::catalog {

enum class CatalogVisibilityFlag : std::uint32_t {
    None = 0,
    Frozen = 1U << 0,
    Invalid = 1U << 1,
    HintCommitted = 1U << 2,
    HintDeleted = 1U << 3
};

constexpr CatalogVisibilityFlag operator|(CatalogVisibilityFlag lhs, CatalogVisibilityFlag rhs) noexcept
{
    return static_cast<CatalogVisibilityFlag>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
}

constexpr CatalogVisibilityFlag operator&(CatalogVisibilityFlag lhs, CatalogVisibilityFlag rhs) noexcept
{
    return static_cast<CatalogVisibilityFlag>(static_cast<std::uint32_t>(lhs) & static_cast<std::uint32_t>(rhs));
}

constexpr bool any(CatalogVisibilityFlag value) noexcept
{
    return static_cast<std::uint32_t>(value) != 0U;
}

constexpr std::uint32_t to_value(CatalogVisibilityFlag flag) noexcept
{
    return static_cast<std::uint32_t>(flag);
}

constexpr bool has_flag(std::uint32_t mask, CatalogVisibilityFlag flag) noexcept
{
    return (mask & to_value(flag)) != 0U;
}

constexpr void set_flag(std::uint32_t& mask, CatalogVisibilityFlag flag) noexcept
{
    mask |= to_value(flag);
}

constexpr void clear_flag(std::uint32_t& mask, CatalogVisibilityFlag flag) noexcept
{
    mask &= ~to_value(flag);
}

struct CatalogSnapshot final {
    std::uint64_t xmin = 0U;
    std::uint64_t xmax = 0U;
    std::vector<std::uint64_t> in_progress{};

    CatalogSnapshot() = default;
    explicit CatalogSnapshot(const txn::Snapshot& snapshot)
        : xmin{snapshot.xmin}
        , xmax{snapshot.xmax}
        , in_progress{snapshot.in_progress.begin(), snapshot.in_progress.end()}
    {
        normalize();
    }

    void normalize()
    {
        std::sort(in_progress.begin(), in_progress.end());
        in_progress.erase(std::unique(in_progress.begin(), in_progress.end()), in_progress.end());
    }

    [[nodiscard]] bool is_in_progress(std::uint64_t transaction_id) const noexcept
    {
        if (transaction_id == 0U || in_progress.empty()) {
            return false;
        }
        return std::binary_search(in_progress.begin(), in_progress.end(), transaction_id);
    }

    [[nodiscard]] bool is_visible_insert(std::uint64_t transaction_id, std::uint64_t reader_transaction_id) const noexcept
    {
        if (transaction_id == 0U) {
            return false;
        }
        if (transaction_id == reader_transaction_id) {
            return true;
        }
        if (transaction_id < xmin) {
            return true;
        }
        if (transaction_id >= xmax) {
            return false;
        }
        return !is_in_progress(transaction_id);
    }

    [[nodiscard]] bool is_visible_delete(std::uint64_t transaction_id, std::uint64_t reader_transaction_id) const noexcept
    {
        if (transaction_id == 0U) {
            return false;
        }
        if (transaction_id == reader_transaction_id) {
            return false;
        }
        if (transaction_id < xmin) {
            return true;
        }
        if (transaction_id >= xmax) {
            return false;
        }
        return !is_in_progress(transaction_id);
    }
};

[[nodiscard]] inline bool is_tuple_visible(const CatalogTupleDescriptor& tuple,
                                           const CatalogSnapshot& snapshot,
                                           std::uint64_t reader_transaction_id) noexcept
{
    if (has_flag(tuple.visibility_flags, CatalogVisibilityFlag::Invalid)) {
        return false;
    }

    if (tuple.xmin == 0U) {
        return false;
    }

    const bool insert_visible = snapshot.is_visible_insert(tuple.xmin, reader_transaction_id);
    const bool self_insert = tuple.xmin == reader_transaction_id;

    if (!insert_visible && !self_insert) {
        if (has_flag(tuple.visibility_flags, CatalogVisibilityFlag::Frozen)) {
            return !snapshot.is_visible_delete(tuple.xmax, reader_transaction_id);
        }
        return false;
    }

    if (tuple.xmax == 0U) {
        return true;
    }

    if (tuple.xmax == reader_transaction_id) {
        return false;
    }

    if (has_flag(tuple.visibility_flags, CatalogVisibilityFlag::Frozen)) {
        return true;
    }

    return !snapshot.is_visible_delete(tuple.xmax, reader_transaction_id);
}

}  // namespace bored::catalog
