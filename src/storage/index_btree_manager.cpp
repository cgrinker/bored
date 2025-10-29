#include "bored/storage/index_btree_manager.hpp"

#include "bored/storage/page_latch_guard.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/temp_resource_registry.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <span>
#include <system_error>
#include <utility>
#include <vector>

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

struct LeafEntry final {
    IndexBtreeTuplePointer pointer{};
    std::vector<std::byte> key{};
};

[[nodiscard]] bool pointers_equal(const IndexBtreeTuplePointer& lhs, const IndexBtreeTuplePointer& rhs) noexcept
{
    return lhs.heap_page_id == rhs.heap_page_id && lhs.heap_slot_id == rhs.heap_slot_id;
}

[[nodiscard]] std::error_code validate_key_length(const IndexComparatorEntry* comparator,
                                                  std::span<const std::byte> key)
{
    if (key.size() > kIndexBtreeSlotLengthMask) {
        return std::make_error_code(std::errc::value_too_large);
    }

    if (comparator != nullptr && comparator->fixed_length != 0U && key.size() != comparator->fixed_length) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    return {};
}

[[nodiscard]] std::vector<LeafEntry> read_leaf_entries(std::span<const std::byte> page,
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

    std::vector<LeafEntry> entries;
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

        if (key_offset + key_length > static_cast<std::size_t>(page_header(page).free_start)) {
            out_error = make_invalid_page_error();
            return {};
        }

        if (key_offset < pointer_size || key_offset - pointer_size < kIndexPayloadBase) {
            out_error = make_invalid_page_error();
            return {};
        }

        const auto pointer_offset = key_offset - pointer_size;
        LeafEntry entry{};
        std::memcpy(&entry.pointer, page.data() + pointer_offset, pointer_size);
        entry.key.resize(key_length);
        if (key_length != 0U) {
            std::memcpy(entry.key.data(), page.data() + key_offset, key_length);
        }
        entries.push_back(std::move(entry));
    }

    out_error.clear();
    return entries;
}

[[nodiscard]] std::size_t total_payload_size(std::span<const LeafEntry> entries)
{
    const auto pointer_size = sizeof(IndexBtreeTuplePointer);
    std::size_t total = 0U;
    for (const auto& entry : entries) {
        total += pointer_size + entry.key.size();
    }
    return total;
}

[[nodiscard]] bool rebuild_leaf_page(std::span<std::byte> page,
                                     std::span<const LeafEntry> entries,
                                     std::uint64_t lsn)
{
    auto& header = page_header(page);
    auto& index = index_header(page);

    if (!index_page_is_leaf(index)) {
        return false;
    }

    const auto pointer_size = sizeof(IndexBtreeTuplePointer);
    const auto slot_size = sizeof(IndexBtreeSlotEntry);
    const auto payload_bytes = total_payload_size(std::span<const LeafEntry>(entries.data(), entries.size()));
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

[[nodiscard]] std::size_t find_insertion_position(const std::vector<LeafEntry>& entries,
                                                  std::span<const std::byte> key,
                                                  const IndexComparatorEntry* comparator,
                                                  bool allow_duplicates,
                                                  bool& duplicate) noexcept
{
    duplicate = false;
    std::size_t position = 0U;

    for (; position < entries.size(); ++position) {
        const auto order = comparator->compare(entries[position].key, key);
        if (order > 0) {
            break;
        }
        if (order == 0) {
            duplicate = true;
            if (!allow_duplicates) {
                break;
            }
        }
    }

    return position;
}

[[nodiscard]] std::size_t locate_pointer(const std::vector<LeafEntry>& entries,
                                         const IndexBtreeTuplePointer& pointer,
                                         std::span<const std::byte> key,
                                         const IndexComparatorEntry* comparator)
{
    for (std::size_t index = 0; index < entries.size(); ++index) {
        if (comparator->compare(entries[index].key, key) == 0 && pointers_equal(entries[index].pointer, pointer)) {
            return index;
        }
    }
    return entries.size();
}

[[nodiscard]] std::span<const std::byte> payload_span(std::span<const std::byte> page)
{
    const auto& header = page_header(page);
    if (header.free_start <= kIndexPayloadBase) {
        return {};
    }
    return page.subspan(kIndexPayloadBase, static_cast<std::size_t>(header.free_start) - kIndexPayloadBase);
}

}  // namespace

IndexBtreeManager::IndexBtreeManager(IndexBtreeManagerConfig config)
    : config_{std::move(config)}
    , comparator_{config_.comparator}
    , temp_registry_{config_.temp_registry}
{
}

std::error_code IndexBtreeManager::validate_leaf_page(std::span<const std::byte> page) const noexcept
{
    if (comparator_ == nullptr) {
        return std::make_error_code(std::errc::not_supported);
    }

    if (page.size() != kPageSize) {
        return make_invalid_page_error();
    }

    const auto& header = page_header(page);
    if (!is_valid(header) || !is_index_page(header)) {
        return make_invalid_page_error();
    }

    const auto& index = index_header(page);
    if (index.magic != kIndexPageMagic || index.version != kIndexPageVersion) {
        return make_invalid_page_error();
    }

    if (!index_page_is_leaf(index)) {
        return std::make_error_code(std::errc::operation_not_supported);
    }

    if (static_cast<std::size_t>(header.free_start) < kIndexPayloadBase) {
        return make_invalid_page_error();
    }

    return {};
}

std::error_code IndexBtreeManager::ensure_scratch_directory()
{
    if (temp_registry_ == nullptr) {
        return {};
    }

    if (!scratch_directory_.empty()) {
        return {};
    }

    const auto tag = config_.scratch_tag.empty() ? std::string{"index"} : config_.scratch_tag;
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto base = std::filesystem::temp_directory_path();
    std::filesystem::path candidate = base / ("bored_" + tag + "_scratch_" + std::to_string(now)
                                              + "_" + std::to_string(reinterpret_cast<std::uintptr_t>(this)));

    std::error_code mkdir_error;
    std::filesystem::create_directories(candidate, mkdir_error);
    if (mkdir_error) {
        return mkdir_error;
    }

    temp_registry_->register_directory(candidate);
    scratch_directory_ = std::move(candidate);
    return {};
}

void IndexBtreeManager::maybe_record_scratch(std::uint32_t page_id, std::span<const std::byte> payload)
{
    if (temp_registry_ == nullptr || payload.empty()) {
        return;
    }

    if (auto ec = ensure_scratch_directory(); ec) {
        return;
    }

    if (scratch_directory_.empty()) {
        return;
    }

    auto file_path = scratch_directory_ / ("page_" + std::to_string(page_id) + "_" + std::to_string(scratch_sequence_++) + ".bin");
    std::ofstream stream(file_path, std::ios::binary | std::ios::trunc);
    if (!stream) {
        return;
    }

    stream.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    stream.close();
    temp_registry_->register_file(file_path);
}

std::error_code IndexBtreeManager::insert_leaf(std::span<std::byte> page,
                                               std::uint32_t page_id,
                                               IndexBtreeTuplePointer tuple_pointer,
                                               std::span<const std::byte> key,
                                               std::uint64_t lsn,
                                               IndexInsertOptions options,
                                               IndexInsertResult& out_result)
{
    out_result = IndexInsertResult{};

    auto latch_guard = PageLatchGuard{&config_.latch_callbacks, page_id, PageLatchMode::Exclusive};
    if (auto latch_error = latch_guard.status(); latch_error) {
        return latch_error;
    }

    if (auto validation_error = validate_leaf_page(page); validation_error) {
        return validation_error;
    }

    if (auto key_error = validate_key_length(comparator_, key); key_error) {
        return key_error;
    }

    std::error_code parse_error;
    auto entries = read_leaf_entries(page, parse_error);
    if (parse_error) {
        return parse_error;
    }

    bool duplicate = false;
    const auto insert_pos = find_insertion_position(entries, key, comparator_, options.allow_duplicates, duplicate);
    if (duplicate && !options.allow_duplicates) {
        return std::make_error_code(std::errc::file_exists);
    }

    LeafEntry entry{};
    entry.pointer = tuple_pointer;
    entry.key.assign(key.begin(), key.end());

    entries.insert(entries.begin() + static_cast<std::ptrdiff_t>(insert_pos), std::move(entry));

    const auto payload_bytes = total_payload_size(std::span<const LeafEntry>(entries.data(), entries.size()));
    const auto slot_bytes = entries.size() * sizeof(IndexBtreeSlotEntry);
    if (kIndexPayloadBase + payload_bytes + slot_bytes > page.size()) {
        out_result.requires_split = true;
        return std::make_error_code(std::errc::no_buffer_space);
    }

    if (!rebuild_leaf_page(page, std::span<const LeafEntry>(entries.data(), entries.size()), lsn)) {
        out_result.requires_split = true;
        return std::make_error_code(std::errc::no_buffer_space);
    }

    out_result.slot_index = static_cast<std::uint16_t>(insert_pos);
    out_result.inserted = true;

    maybe_record_scratch(page_id, payload_span(page));

    return {};
}

std::error_code IndexBtreeManager::delete_leaf(std::span<std::byte> page,
                                               std::uint32_t page_id,
                                               std::span<const std::byte> key,
                                               IndexBtreeTuplePointer tuple_pointer,
                                               bool remove_all_duplicates,
                                               std::uint64_t lsn,
                                               IndexDeleteResult& out_result)
{
    out_result = IndexDeleteResult{};

    auto latch_guard = PageLatchGuard{&config_.latch_callbacks, page_id, PageLatchMode::Exclusive};
    if (auto latch_error = latch_guard.status(); latch_error) {
        return latch_error;
    }

    if (auto validation_error = validate_leaf_page(page); validation_error) {
        return validation_error;
    }

    if (auto key_error = validate_key_length(comparator_, key); key_error) {
        return key_error;
    }

    std::error_code parse_error;
    auto entries = read_leaf_entries(page, parse_error);
    if (parse_error) {
        return parse_error;
    }

    std::size_t removed = 0U;
    auto it = entries.begin();
    while (it != entries.end()) {
        const auto order = comparator_->compare(it->key, key);
        if (order < 0) {
            ++it;
            continue;
        }
        if (order > 0) {
            break;
        }

        if (pointers_equal(it->pointer, tuple_pointer)) {
            it = entries.erase(it);
            ++removed;
            if (!remove_all_duplicates) {
                break;
            }
            continue;
        }
        ++it;
    }

    if (removed == 0U) {
        out_result.removed_count = 0U;
        out_result.page_empty = entries.empty();
        return std::make_error_code(std::errc::no_such_file_or_directory);
    }

    if (!rebuild_leaf_page(page, std::span<const LeafEntry>(entries.data(), entries.size()), lsn)) {
        return std::make_error_code(std::errc::io_error);
    }

    out_result.removed_count = removed;
    out_result.page_empty = entries.empty();

    maybe_record_scratch(page_id, payload_span(page));

    return {};
}

std::error_code IndexBtreeManager::update_leaf(std::span<std::byte> page,
                                               std::uint32_t page_id,
                                               std::uint16_t slot_index,
                                               IndexBtreeTuplePointer tuple_pointer,
                                               std::span<const std::byte> new_key,
                                               std::uint64_t lsn,
                                               IndexUpdateResult& out_result)
{
    out_result = IndexUpdateResult{};

    auto latch_guard = PageLatchGuard{&config_.latch_callbacks, page_id, PageLatchMode::Exclusive};
    if (auto latch_error = latch_guard.status(); latch_error) {
        return latch_error;
    }

    if (auto validation_error = validate_leaf_page(page); validation_error) {
        return validation_error;
    }

    if (auto key_error = validate_key_length(comparator_, new_key); key_error) {
        return key_error;
    }

    std::error_code parse_error;
    auto entries = read_leaf_entries(page, parse_error);
    if (parse_error) {
        return parse_error;
    }

    if (slot_index >= entries.size()) {
        return make_invalid_page_error();
    }

    LeafEntry entry = entries[slot_index];
    entries.erase(entries.begin() + slot_index);
    entry.pointer = tuple_pointer;
    entry.key.assign(new_key.begin(), new_key.end());

    bool duplicate = false;
    const auto insert_pos = find_insertion_position(entries, entry.key, comparator_, true, duplicate);
    entries.insert(entries.begin() + static_cast<std::ptrdiff_t>(insert_pos), std::move(entry));

    const auto payload_bytes = total_payload_size(entries);
    const auto slot_bytes = entries.size() * sizeof(IndexBtreeSlotEntry);
    if (kIndexPayloadBase + payload_bytes + slot_bytes > page.size()) {
        out_result.requires_split = true;
        return std::make_error_code(std::errc::no_buffer_space);
    }

    if (!rebuild_leaf_page(page, std::span<const LeafEntry>(entries.data(), entries.size()), lsn)) {
        out_result.requires_split = true;
        return std::make_error_code(std::errc::no_buffer_space);
    }

    out_result.slot_index = static_cast<std::uint16_t>(locate_pointer(entries, tuple_pointer, new_key, comparator_));
    out_result.updated = true;

    maybe_record_scratch(page_id, payload_span(page));

    return {};
}

IndexSearchResult IndexBtreeManager::find_leaf_slot(std::span<const std::byte> page,
                                                    std::span<const std::byte> key,
                                                    IndexBtreeTuplePointer tuple_pointer) const
{
    IndexSearchResult result{};

    if (auto validation_error = validate_leaf_page(page); validation_error) {
        return result;
    }

    if (auto key_error = validate_key_length(comparator_, key); key_error) {
        return result;
    }

    std::error_code parse_error;
    auto entries = read_leaf_entries(page, parse_error);
    if (parse_error) {
        return result;
    }

    for (std::size_t index = 0; index < entries.size(); ++index) {
        const auto order = comparator_->compare(entries[index].key, key);
        if (order > 0) {
            break;
        }
        if (order == 0 && pointers_equal(entries[index].pointer, tuple_pointer)) {
            result.found = true;
            result.slot_index = static_cast<std::uint16_t>(index);
            break;
        }
    }

    return result;
}

std::error_code initialize_index_page(std::span<std::byte> page,
                                      std::uint32_t page_id,
                                      std::uint16_t level,
                                      bool is_leaf,
                                      std::uint64_t base_lsn,
                                      FreeSpaceMap* fsm)
{
    if (!initialize_page(page, PageType::Index, page_id, base_lsn, fsm)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto& header = page_header(page);
    auto& index = index_header(page);

    index.magic = kIndexPageMagic;
    index.version = kIndexPageVersion;
    index.flags = 0U;
    if (is_leaf) {
        index.flags |= kIndexPageFlagLeaf;
    }
    index.level = level;
    index.parent_page_id = 0U;
    index.right_sibling_page_id = 0U;

    header.free_start = static_cast<std::uint16_t>(kIndexPayloadBase);
    header.free_end = static_cast<std::uint16_t>(kPageSize);
    header.tuple_count = 0U;
    header.fragment_count = 0U;

    return {};
}

}  // namespace bored::storage
