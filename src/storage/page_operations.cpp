#include "bored/storage/page_operations.hpp"

#include "bored/storage/free_space_map.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

namespace bored::storage {

namespace {

constexpr std::uint16_t to_u16(std::size_t value)
{
    return static_cast<std::uint16_t>(value);
}

SlotPointer* locate_slot(PageHeader& header, std::span<std::byte> page, std::size_t index)
{
    if (index >= header.tuple_count) {
        return nullptr;
    }

    auto* end = reinterpret_cast<SlotPointer*>(page.data() + kPageSize);
    return end - static_cast<std::ptrdiff_t>(index + 1U);
}

const SlotPointer* locate_slot(const PageHeader& header, std::span<const std::byte> page, std::size_t index)
{
    if (index >= header.tuple_count) {
        return nullptr;
    }

    auto* end = reinterpret_cast<const SlotPointer*>(page.data() + kPageSize);
    return end - static_cast<std::ptrdiff_t>(index + 1U);
}

std::optional<std::size_t> find_reusable_slot(const PageHeader& header, std::span<const std::byte> page)
{
    for (std::size_t index = 0; index < header.tuple_count; ++index) {
        const auto* slot = locate_slot(header, page, index);
        if (slot != nullptr && slot->length == 0U) {
            return index;
        }
    }
    return std::nullopt;
}

void mark_dirty(PageHeader& header)
{
    header.flags |= static_cast<std::uint16_t>(PageFlag::Dirty);
}

}  // namespace

PageHeader& page_header(std::span<std::byte> page)
{
    return *reinterpret_cast<PageHeader*>(page.data());
}

const PageHeader& page_header(std::span<const std::byte> page)
{
    return *reinterpret_cast<const PageHeader*>(page.data());
}

std::span<SlotPointer> slot_directory(std::span<std::byte> page)
{
    auto& header = page_header(page);
    auto count = static_cast<std::size_t>(header.tuple_count);
    auto* end = reinterpret_cast<SlotPointer*>(page.data() + kPageSize);
    return {end - static_cast<std::ptrdiff_t>(count), count};
}

std::span<const SlotPointer> slot_directory(std::span<const std::byte> page)
{
    const auto& header = page_header(page);
    auto count = static_cast<std::size_t>(header.tuple_count);
    auto* end = reinterpret_cast<const SlotPointer*>(page.data() + kPageSize);
    return {end - static_cast<std::ptrdiff_t>(count), count};
}

bool initialize_page(std::span<std::byte> page,
                     PageType type,
                     std::uint32_t page_id,
                     std::uint64_t lsn,
                     FreeSpaceMap* fsm)
{
    if (page.size() != kPageSize) {
        return false;
    }

    std::memset(page.data(), 0, page.size());

    auto& header = page_header(page);
    header.magic = kPageMagic;
    header.version = kPageVersion;
    header.type = static_cast<std::uint8_t>(type);
    header.page_id = page_id;
    header.lsn = lsn;
    header.free_start = to_u16(sizeof(PageHeader));
    header.free_end = to_u16(kPageSize);
    header.tuple_count = 0U;
    header.fragment_count = 0U;
    header.flags = static_cast<std::uint16_t>(PageFlag::Dirty);

    if (fsm != nullptr) {
        sync_free_space(*fsm, page);
    }

    return true;
}

std::optional<TupleSlot> append_tuple(std::span<std::byte> page,
                                      std::span<const std::byte> payload,
                                      std::uint64_t lsn,
                                      FreeSpaceMap* fsm)
{
    if (page.size() != kPageSize) {
        return std::nullopt;
    }

    auto& header = page_header(page);
    if (!is_valid(header)) {
        return std::nullopt;
    }

    if (payload.empty() || payload.size() > std::numeric_limits<std::uint16_t>::max()) {
        return std::nullopt;
    }

    auto reusable = find_reusable_slot(header, page);
    const std::size_t slot_overhead = reusable ? 0U : sizeof(SlotPointer);
    if (compute_free_bytes(header) < payload.size() + slot_overhead) {
        return std::nullopt;
    }

    const auto payload_length = to_u16(payload.size());
    const auto write_offset = header.free_start;
    std::memcpy(page.data() + write_offset, payload.data(), payload.size());
    header.free_start = to_u16(static_cast<std::size_t>(header.free_start) + payload.size());

    mark_dirty(header);
    header.lsn = lsn;

    if (reusable) {
        auto* slot = locate_slot(header, page, *reusable);
        slot->offset = write_offset;
        slot->length = payload_length;
        if (header.fragment_count > 0U) {
            --header.fragment_count;
        }

        if (fsm != nullptr) {
            sync_free_space(*fsm, page);
        }

        return TupleSlot{to_u16(*reusable), write_offset, payload_length};
    }

    header.free_end = to_u16(static_cast<std::size_t>(header.free_end) - sizeof(SlotPointer));
    auto* new_slot = reinterpret_cast<SlotPointer*>(page.data() + header.free_end);
    new_slot->offset = write_offset;
    new_slot->length = payload_length;

    const std::uint16_t slot_index = header.tuple_count;
    ++header.tuple_count;

    if (fsm != nullptr) {
        sync_free_space(*fsm, page);
    }

    return TupleSlot{slot_index, write_offset, payload_length};
}

std::optional<AppendTuplePlan> prepare_append_tuple(std::span<const std::byte> page,
                                                    std::size_t payload_length)
{
    if (page.size() != kPageSize) {
        return std::nullopt;
    }

    const auto& header = page_header(page);
    if (!is_valid(header)) {
        return std::nullopt;
    }

    if (payload_length == 0U || payload_length > std::numeric_limits<std::uint16_t>::max()) {
        return std::nullopt;
    }

    auto reusable = find_reusable_slot(header, page);
    const std::size_t slot_overhead = reusable ? 0U : sizeof(SlotPointer);
    if (compute_free_bytes(header) < payload_length + slot_overhead) {
        return std::nullopt;
    }

    AppendTuplePlan plan{};
    plan.slot_index = reusable ? static_cast<std::uint16_t>(*reusable) : header.tuple_count;
    plan.write_offset = header.free_start;
    plan.tuple_length = static_cast<std::uint16_t>(payload_length);
    plan.reuses_slot = reusable.has_value();
    return plan;
}

bool delete_tuple(std::span<std::byte> page,
                  std::uint16_t slot_index,
                  std::uint64_t lsn,
                  FreeSpaceMap* fsm)
{
    if (page.size() != kPageSize) {
        return false;
    }

    auto& header = page_header(page);
    if (!is_valid(header) || slot_index >= header.tuple_count) {
        return false;
    }

    auto* slot = locate_slot(header, page, slot_index);
    if (slot == nullptr || slot->length == 0U) {
        return false;
    }

    slot->length = 0U;
    ++header.fragment_count;
    mark_dirty(header);
    header.lsn = lsn;

    if (fsm != nullptr) {
        sync_free_space(*fsm, page);
    }

    return true;
}

std::span<const std::byte> read_tuple(std::span<const std::byte> page, std::uint16_t slot_index)
{
    if (page.size() != kPageSize) {
        return {};
    }

    const auto& header = page_header(page);
    if (!is_valid(header) || slot_index >= header.tuple_count) {
        return {};
    }

    const auto* slot = locate_slot(header, page, slot_index);
    if (slot == nullptr || slot->length == 0U) {
        return {};
    }

    return {page.data() + slot->offset, slot->length};
}

bool compact_page(std::span<std::byte> page,
                  std::uint64_t lsn,
                  FreeSpaceMap* fsm)
{
    if (page.size() != kPageSize) {
        return false;
    }

    auto& header = page_header(page);
    if (!is_valid(header)) {
        return false;
    }

    std::uint16_t write_cursor = static_cast<std::uint16_t>(sizeof(PageHeader));
    for (std::size_t index = 0; index < header.tuple_count; ++index) {
        auto* slot = locate_slot(header, page, index);
        if (slot == nullptr || slot->length == 0U) {
            continue;
        }

        if (slot->offset != write_cursor) {
            std::memmove(page.data() + write_cursor, page.data() + slot->offset, slot->length);
        }

        slot->offset = write_cursor;
        write_cursor = static_cast<std::uint16_t>(write_cursor + slot->length);
    }

    header.free_start = write_cursor;
    header.fragment_count = 0U;
    mark_dirty(header);
    header.lsn = lsn;

    if (fsm != nullptr) {
        sync_free_space(*fsm, page);
    }

    return true;
}

void sync_free_space(FreeSpaceMap& fsm, std::span<const std::byte> page)
{
    const auto& header = page_header(page);
    fsm.record_page(header.page_id,
                    static_cast<std::uint16_t>(compute_free_bytes(header)),
                    header.fragment_count);
}

bool clear_overflow_page(std::span<std::byte> page,
                         std::uint32_t page_id,
                         std::uint64_t lsn,
                         FreeSpaceMap* fsm)
{
    if (page.size() != kPageSize) {
        return false;
    }

    if (!initialize_page(page, PageType::Overflow, page_id, lsn, fsm)) {
        return false;
    }

    auto& header = page_header(page);
    header.flags = static_cast<std::uint16_t>(PageFlag::Dirty);
    return true;
}

bool write_overflow_chunk(std::span<std::byte> page,
                          const WalOverflowChunkMeta& meta,
                          std::span<const std::byte> payload,
                          std::uint64_t lsn,
                          FreeSpaceMap* fsm)
{
    if (page.size() != kPageSize) {
        return false;
    }

    constexpr std::size_t meta_offset = sizeof(PageHeader);
    constexpr std::size_t meta_size = sizeof(WalOverflowChunkMeta);
    constexpr std::size_t payload_offset = meta_offset + meta_size;
    constexpr std::size_t payload_capacity = kPageSize - payload_offset;

    if (meta.overflow_page_id == 0U) {
        return false;
    }

    if (meta.chunk_length != payload.size()) {
        return false;
    }

    if (meta.chunk_length > payload_capacity) {
        return false;
    }

    if (!clear_overflow_page(page, meta.overflow_page_id, lsn, fsm)) {
        return false;
    }

    std::memcpy(page.data() + meta_offset, &meta, meta_size);
    if (!payload.empty()) {
        std::memcpy(page.data() + payload_offset, payload.data(), payload.size());
    }

    auto& header = page_header(page);
    header.free_start = to_u16(payload_offset + payload.size());
    header.lsn = lsn;
    header.flags = static_cast<std::uint16_t>(PageFlag::Dirty);

    if (fsm != nullptr) {
        sync_free_space(*fsm, page);
    }

    return true;
}

std::optional<WalOverflowChunkMeta> read_overflow_chunk_meta(std::span<const std::byte> page)
{
    if (page.size() != kPageSize) {
        return std::nullopt;
    }

    const auto& header = page_header(page);
    if (!is_valid(header)) {
        return std::nullopt;
    }

    if (static_cast<PageType>(header.type) != PageType::Overflow) {
        return std::nullopt;
    }

    constexpr std::size_t meta_offset = sizeof(PageHeader);
    constexpr std::size_t meta_size = sizeof(WalOverflowChunkMeta);
    if (header.free_start < meta_offset + meta_size) {
        return std::nullopt;
    }

    WalOverflowChunkMeta meta{};
    std::memcpy(&meta, page.data() + meta_offset, meta_size);

    constexpr std::size_t payload_capacity = kPageSize - meta_offset - meta_size;
    if (meta.chunk_length > payload_capacity) {
        return std::nullopt;
    }

    return meta;
}

std::span<const std::byte> overflow_chunk_payload(std::span<const std::byte> page,
                                                   const WalOverflowChunkMeta& meta)
{
    constexpr std::size_t payload_offset = sizeof(PageHeader) + sizeof(WalOverflowChunkMeta);
    if (payload_offset + meta.chunk_length > page.size()) {
        return {};
    }

    return page.subspan(payload_offset, meta.chunk_length);
}

}  // namespace bored::storage
