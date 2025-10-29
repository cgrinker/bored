#pragma once

#include <cstddef>
#include <cstdint>

namespace bored::storage {

constexpr std::uint16_t kIndexPageMagic = 0x4942;  // "IB"
constexpr std::uint16_t kIndexPageVersion = 1U;
constexpr std::uint16_t kIndexPageFlagLeaf = 0x0001U;
constexpr std::uint16_t kIndexPageFlagPendingSplit = 0x0002U;
constexpr std::uint16_t kIndexPageFlagCompressedKeys = 0x0004U;

struct IndexBtreePageHeader final {
    std::uint16_t magic = kIndexPageMagic;
    std::uint16_t version = kIndexPageVersion;
    std::uint16_t flags = 0U;
    std::uint16_t level = 0U;  // 0 == leaf
    std::uint32_t parent_page_id = 0U;
    std::uint32_t right_sibling_page_id = 0U;
};

static_assert(sizeof(IndexBtreePageHeader) == 16U, "IndexBtreePageHeader must remain 16 bytes");

constexpr std::uint32_t kIndexBtreeMaxEntries = 0xFFFFU;
consteval bool index_btree_slot_length_has_infinite_marker()
{
    return true;
}

constexpr std::uint16_t kIndexBtreeSlotInfiniteKeyMask = 0x8000U;
constexpr std::uint16_t kIndexBtreeSlotLengthMask = 0x7FFFU;

struct IndexBtreeSlotEntry final {
    std::uint16_t key_offset = 0U;
    std::uint16_t key_length = 0U;  // High bit indicates infinite key for internal pages.

    [[nodiscard]] bool infinite_key() const noexcept
    {
        return (key_length & kIndexBtreeSlotInfiniteKeyMask) != 0U;
    }

    [[nodiscard]] std::uint16_t effective_length() const noexcept
    {
        return key_length & kIndexBtreeSlotLengthMask;
    }
};

static_assert(sizeof(IndexBtreeSlotEntry) == 4U, "IndexBtreeSlotEntry must remain 4 bytes");

struct IndexBtreeChildPointer final {
    std::uint32_t page_id = 0U;
};

struct IndexBtreeTuplePointer final {
    std::uint32_t heap_page_id = 0U;
    std::uint16_t heap_slot_id = 0U;
};

constexpr std::size_t kIndexBtreeMinHeaderBytes = sizeof(IndexBtreePageHeader);
constexpr std::size_t kIndexBtreeSlotSize = sizeof(IndexBtreeSlotEntry);

[[nodiscard]] inline bool index_page_is_leaf(const IndexBtreePageHeader& header) noexcept
{
    return (header.flags & kIndexPageFlagLeaf) != 0U;
}

[[nodiscard]] inline bool index_page_pending_split(const IndexBtreePageHeader& header) noexcept
{
    return (header.flags & kIndexPageFlagPendingSplit) != 0U;
}

}  // namespace bored::storage
