#include "bored/storage/index_btree_leaf_ops.hpp"

#include "bored/storage/page_operations.hpp"

#include <cstring>

namespace bored::storage {
namespace {

constexpr std::size_t kIndexHeaderOffset = sizeof(PageHeader);
constexpr std::size_t kIndexPayloadBase = sizeof(PageHeader) + sizeof(IndexBtreePageHeader);

[[nodiscard]] IndexBtreePageHeader& index_header(std::span<std::byte> page) noexcept
{
    return *reinterpret_cast<IndexBtreePageHeader*>(page.data() + kIndexHeaderOffset);
}

[[nodiscard]] const IndexBtreePageHeader& index_header(std::span<const std::byte> page) noexcept
{
    return *reinterpret_cast<const IndexBtreePageHeader*>(page.data() + kIndexHeaderOffset);
}

[[nodiscard]] bool is_index_page(const PageHeader& header) noexcept
{
    return static_cast<PageType>(header.type) == PageType::Index;
}

[[nodiscard]] std::error_code make_invalid_page_error()
{
    return std::make_error_code(std::errc::invalid_argument);
}

}  // namespace

std::vector<IndexBtreeLeafEntry> read_index_leaf_entries(std::span<const std::byte> page,
                                                         std::error_code& out_error)
{
    const auto& header = page_header(page);
    const auto& index = index_header(page);

    if (!is_index_page(header) || !index_page_is_leaf(index)) {
        out_error = make_invalid_page_error();
        return {};
    }

    if (header.tuple_count == 0U) {
        out_error.clear();
        return {};
    }

    const auto pointer_size = sizeof(IndexBtreeTuplePointer);
    const auto slot_count = static_cast<std::size_t>(header.tuple_count);
    const auto directory_begin = reinterpret_cast<const IndexBtreeSlotEntry*>(page.data() + header.free_end);

    std::vector<IndexBtreeLeafEntry> entries;
    entries.reserve(slot_count);

    for (std::size_t logical_index = 0; logical_index < slot_count; ++logical_index) {
        const auto& slot = directory_begin[logical_index];
        if (slot.infinite_key()) {
            out_error = std::make_error_code(std::errc::operation_not_supported);
            return {};
        }

        const auto key_length = static_cast<std::size_t>(slot.effective_length());
        const auto key_offset = static_cast<std::size_t>(slot.key_offset);
        if (key_offset < kIndexPayloadBase || key_offset > page.size()) {
            out_error = make_invalid_page_error();
            return {};
        }

        if (key_offset + key_length > static_cast<std::size_t>(header.free_start)) {
            out_error = make_invalid_page_error();
            return {};
        }

        if (key_offset < pointer_size || key_offset - pointer_size < kIndexPayloadBase) {
            out_error = make_invalid_page_error();
            return {};
        }

        const auto pointer_offset = key_offset - pointer_size;
        IndexBtreeLeafEntry entry{};
        std::memcpy(&entry.pointer, page.data() + pointer_offset, pointer_size);
        entry.key.resize(key_length);
        if (key_length != 0U) {
            std::memcpy(entry.key.data(), page.data() + key_offset, key_length);
        }
        entry.pointer_offset = static_cast<std::uint16_t>(pointer_offset);
        entry.key_offset = static_cast<std::uint16_t>(slot.key_offset);
        entries.push_back(std::move(entry));
    }

    out_error.clear();
    return entries;
}

std::size_t index_leaf_payload_size(std::span<const IndexBtreeLeafEntry> entries)
{
    const auto pointer_size = sizeof(IndexBtreeTuplePointer);
    std::size_t total = 0U;
    for (const auto& entry : entries) {
        total += pointer_size + entry.key.size();
    }
    return total;
}

bool rebuild_index_leaf_page(std::span<std::byte> page,
                             std::span<const IndexBtreeLeafEntry> entries,
                             std::uint64_t lsn)
{
    auto& header = page_header(page);
    auto& index = index_header(page);

    if (!index_page_is_leaf(index)) {
        return false;
    }

    const auto pointer_size = sizeof(IndexBtreeTuplePointer);
    const auto slot_size = sizeof(IndexBtreeSlotEntry);
    const auto payload_bytes = index_leaf_payload_size(entries);
    const auto slot_bytes = static_cast<std::size_t>(entries.size()) * slot_size;

    if (kIndexPayloadBase + payload_bytes + slot_bytes > page.size()) {
        return false;
    }

    const auto slot_start = page.size() - slot_bytes;
    const auto payload_end = kIndexPayloadBase + payload_bytes;

    auto* slots = reinterpret_cast<IndexBtreeSlotEntry*>(page.data() + slot_start);
    std::size_t write_cursor = kIndexPayloadBase;

    for (std::size_t logical_index = 0; logical_index < entries.size(); ++logical_index) {
        const auto& entry = entries[logical_index];
        std::memcpy(page.data() + write_cursor, &entry.pointer, pointer_size);
        const auto key_offset = write_cursor + pointer_size;
        if (!entry.key.empty()) {
            std::memcpy(page.data() + key_offset, entry.key.data(), entry.key.size());
        }

        slots[logical_index].key_offset = static_cast<std::uint16_t>(key_offset);
        slots[logical_index].key_length = static_cast<std::uint16_t>(entry.key.size());

        write_cursor = key_offset + entry.key.size();
    }

    header.free_start = static_cast<std::uint16_t>(payload_end);
    header.free_end = static_cast<std::uint16_t>(slot_start);
    header.tuple_count = static_cast<std::uint16_t>(entries.size());
    header.fragment_count = 0U;
    header.flags |= static_cast<std::uint16_t>(PageFlag::Dirty);
    header.lsn = lsn;

    return true;
}

}  // namespace bored::storage
