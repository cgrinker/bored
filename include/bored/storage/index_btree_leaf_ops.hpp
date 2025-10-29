#pragma once

#include "bored/storage/index_btree_page.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <system_error>
#include <vector>

namespace bored::storage {

struct IndexBtreeLeafEntry final {
    IndexBtreeTuplePointer pointer{};
    std::vector<std::byte> key{};
    std::uint16_t pointer_offset = 0U;
    std::uint16_t key_offset = 0U;
};

[[nodiscard]] std::vector<IndexBtreeLeafEntry> read_index_leaf_entries(std::span<const std::byte> page,
                                                                       std::error_code& out_error);

[[nodiscard]] std::size_t index_leaf_payload_size(std::span<const IndexBtreeLeafEntry> entries);

[[nodiscard]] bool rebuild_index_leaf_page(std::span<std::byte> page,
                                           std::span<const IndexBtreeLeafEntry> entries,
                                           std::uint64_t lsn);

}  // namespace bored::storage
