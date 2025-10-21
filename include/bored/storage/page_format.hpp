#pragma once

#include <cstddef>
#include <cstdint>

namespace bored::storage {

constexpr std::size_t kPageSize = 8192;
constexpr std::uint32_t kPageMagic = 0xB0EDFACE;
constexpr std::uint16_t kPageVersion = 1;

enum class PageType : std::uint8_t {
    Free = 0,
    Meta = 1,
    Table = 2,
    Index = 3,
    Overflow = 4
};

enum class PageFlag : std::uint16_t {
    None = 0,
    Dirty = 1 << 0,
    HasOverflow = 1 << 1
};

constexpr PageFlag operator|(PageFlag lhs, PageFlag rhs)
{
    return static_cast<PageFlag>(static_cast<std::uint16_t>(lhs) | static_cast<std::uint16_t>(rhs));
}

constexpr PageFlag operator&(PageFlag lhs, PageFlag rhs)
{
    return static_cast<PageFlag>(static_cast<std::uint16_t>(lhs) & static_cast<std::uint16_t>(rhs));
}

constexpr bool any(PageFlag value)
{
    return static_cast<std::uint16_t>(value) != 0U;
}

struct alignas(8) PageHeader final {
    std::uint32_t magic = kPageMagic;
    std::uint16_t version = kPageVersion;
    std::uint8_t type = static_cast<std::uint8_t>(PageType::Free);
    std::uint8_t reserved = 0U;
    std::uint32_t page_id = 0U;
    std::uint64_t lsn = 0U;
    std::uint16_t checksum = 0U;
    std::uint16_t flags = 0U;
    std::uint16_t free_start = static_cast<std::uint16_t>(sizeof(PageHeader));
    std::uint16_t free_end = static_cast<std::uint16_t>(kPageSize);
    std::uint16_t tuple_count = 0U;
    std::uint16_t fragment_count = 0U;
};

struct SlotPointer final {
    std::uint16_t offset = 0U;
    std::uint16_t length = 0U;
};

constexpr std::size_t header_size()
{
    return sizeof(PageHeader);
}

constexpr std::size_t max_slot_count()
{
    return (kPageSize - header_size()) / sizeof(SlotPointer);
}

constexpr std::size_t compute_free_bytes(const PageHeader& header)
{
    return static_cast<std::size_t>(header.free_end) - static_cast<std::size_t>(header.free_start);
}

constexpr bool has_flag(const PageHeader& header, PageFlag flag)
{
    return (header.flags & static_cast<std::uint16_t>(flag)) != 0U;
}

constexpr bool is_valid(const PageHeader& header)
{
    return header.magic == kPageMagic && header.version == kPageVersion && header.free_start >= header_size() && header.free_end <= kPageSize && header.free_start <= header.free_end;
}

static_assert(sizeof(PageHeader) == 40, "PageHeader expected to be 40 bytes");
static_assert(alignof(PageHeader) == 8, "PageHeader alignment must be 8");
static_assert(sizeof(SlotPointer) == 4, "SlotPointer expected to be 4 bytes");
static_assert(kPageSize == 8192, "Page size should remain constant");
static_assert(max_slot_count() >= 2000, "Page must support many slots");

}  // namespace bored::storage
