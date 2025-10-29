#include "bored/storage/wal_replayer.hpp"

#include "bored/storage/free_space_map.hpp"
#include "bored/storage/index_btree_page.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/temp_resource_registry.hpp"
#include "bored/storage/wal_apply_helpers.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_undo_walker.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

namespace bored::storage {

namespace {

constexpr std::size_t kIndexHeaderOffset = sizeof(PageHeader);
constexpr std::size_t kIndexPayloadBase = sizeof(PageHeader) + sizeof(IndexBtreePageHeader);

std::span<const std::byte> as_const_span(std::span<std::byte> buffer)
{
    return {buffer.data(), buffer.size()};
}

struct IndexInternalEntry final {
    IndexBtreeChildPointer child{};
    std::vector<std::byte> key{};
    bool infinite = false;
};

constexpr PageType replay_page_type(WalRecordType type) noexcept
{
    switch (type) {
    case WalRecordType::CatalogInsert:
    case WalRecordType::CatalogDelete:
    case WalRecordType::CatalogUpdate:
        return PageType::Meta;
    default:
        return PageType::Table;
    }
}

std::span<std::byte> ensure_page(WalReplayContext& context, std::uint32_t page_id, PageType type)
{
    auto page = context.get_page(page_id);
    auto& header = page_header(page);
    if (!is_valid(header) || header.page_id != page_id || static_cast<PageType>(header.type) != type) {
        if (!initialize_page(page, type, page_id, 0U, context.free_space_map())) {
            throw std::runtime_error{"Failed to initialise replay page"};
        }
    }
    return page;
}

std::error_code apply_overflow_chunk(WalReplayContext& context,
                                     const WalRecoveryRecord& record,
                                     const WalOverflowChunkMeta& meta,
                                     std::span<const std::byte> payload)
{
    if (record.header.page_id != meta.overflow_page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto page = ensure_page(context, meta.overflow_page_id, PageType::Overflow);
    if (wal::page_already_applied(page, record.header)) {
        return {};
    }

    if (!write_overflow_chunk(page, meta, payload, record.header.lsn, context.free_space_map())) {
        return std::make_error_code(std::errc::io_error);
    }

    return {};
}

std::error_code apply_overflow_truncate(WalReplayContext& context,
                                        const WalRecordHeader& header,
                                        const WalOverflowTruncateMeta& meta)
{
    auto fsm = context.free_space_map();
    auto current_page = meta.first_overflow_page_id;

    for (std::uint32_t released = 0U; released < meta.released_page_count && current_page != 0U; ++released) {
        auto page = ensure_page(context, current_page, PageType::Overflow);
        auto existing_meta = read_overflow_chunk_meta(std::span<const std::byte>(page.data(), page.size()));
        auto next_page = existing_meta ? existing_meta->next_overflow_page_id : 0U;

    if (!wal::page_already_applied(page, header)) {
            if (!clear_overflow_page(page, current_page, header.lsn, fsm)) {
                return std::make_error_code(std::errc::io_error);
            }
        }

        current_page = next_page;
    }

    return {};
}

std::error_code undo_overflow_chunk(WalReplayContext& context,
                                    const WalRecoveryRecord& record,
                                    const WalOverflowChunkMeta& meta)
{
    if (record.header.page_id != meta.overflow_page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto page = ensure_page(context, meta.overflow_page_id, PageType::Overflow);
    if (!clear_overflow_page(page, meta.overflow_page_id, record.header.lsn, context.free_space_map())) {
        return std::make_error_code(std::errc::io_error);
    }

    return {};
}

std::error_code undo_overflow_truncate(WalReplayContext& context,
                                       const WalRecoveryRecord& record,
                                       const WalOverflowTruncateMeta& meta)
{
    auto chunk_views = decode_wal_overflow_truncate_chunks(record.payload, meta);
    if (!chunk_views) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    for (const auto& view : *chunk_views) {
        auto page = ensure_page(context, view.meta.overflow_page_id, PageType::Overflow);
        if (!write_overflow_chunk(page, view.meta, view.payload, record.header.lsn, context.free_space_map())) {
            return std::make_error_code(std::errc::io_error);
        }
    }

    return {};
}

std::error_code apply_page_compaction(std::span<std::byte> page,
                                      const WalRecordHeader& header,
                                      const WalCompactionView& view,
                                      FreeSpaceMap* fsm,
                                      bool& applied)
{
    applied = false;

    auto const_page = as_const_span(page);
    if (page_header(const_page).page_id != header.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (wal::page_already_applied(const_page, header)) {
        return {};
    }

    std::vector<std::byte> original(const_page.begin(), const_page.end());
    auto directory = slot_directory(page);
    const auto directory_size = directory.size();

    if (view.header.new_free_start > kPageSize) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    for (const auto& entry : view.entries) {
        if (entry.slot_index >= directory_size) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        const auto directory_index = directory_size - static_cast<std::size_t>(entry.slot_index) - 1U;
        auto& slot = directory[directory_index];
        if (slot.length != entry.length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (static_cast<std::size_t>(entry.old_offset) + entry.length > original.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (static_cast<std::size_t>(entry.new_offset) + entry.length > page.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        if (entry.length != 0U) {
            std::memcpy(page.data() + entry.new_offset, original.data() + entry.old_offset, entry.length);
        }
        slot.offset = static_cast<std::uint16_t>(entry.new_offset);
    }

    auto& header_ref = page_header(page);
    header_ref.free_start = static_cast<std::uint16_t>(view.header.new_free_start);
    header_ref.fragment_count = 0U;
    header_ref.flags |= static_cast<std::uint16_t>(PageFlag::Dirty);
    header_ref.lsn = header.lsn;

    if (fsm != nullptr) {
        sync_free_space(*fsm, page);
    }

    applied = true;
    return {};
}

std::error_code undo_page_compaction(std::span<std::byte> page,
                                     const WalRecordHeader& header,
                                     const WalCompactionView& view,
                                     FreeSpaceMap* fsm)
{
    if (page_header(page).page_id != header.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    std::vector<std::byte> original(page.begin(), page.end());
    auto directory = slot_directory(page);
    const auto directory_size = directory.size();

    for (const auto& entry : view.entries) {
        if (entry.slot_index >= directory_size) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        const auto directory_index = directory_size - static_cast<std::size_t>(entry.slot_index) - 1U;
        auto& slot = directory[directory_index];
        if (slot.length != entry.length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (static_cast<std::size_t>(entry.new_offset) + entry.length > original.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (static_cast<std::size_t>(entry.old_offset) + entry.length > page.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        if (entry.length != 0U) {
            std::memcpy(page.data() + entry.old_offset, original.data() + entry.new_offset, entry.length);
        }
        slot.offset = static_cast<std::uint16_t>(entry.old_offset);
    }

    auto& header_ref = page_header(page);
    header_ref.free_start = static_cast<std::uint16_t>(view.header.old_free_start);
    header_ref.fragment_count = static_cast<std::uint16_t>(view.header.old_fragment_count);
    header_ref.flags |= static_cast<std::uint16_t>(PageFlag::Dirty);
    header_ref.lsn = header.lsn;

    if (fsm != nullptr) {
        sync_free_space(*fsm, page);
    }

    return {};
}

[[nodiscard]] bool has_flag(WalIndexSplitFlag value, WalIndexSplitFlag flag) noexcept
{
    return any(value & flag);
}

std::error_code rebuild_index_page(std::span<std::byte> page,
                                   FreeSpaceMap* fsm,
                                   const WalRecordHeader& wal_header,
                                   std::uint32_t page_id,
                                   std::uint16_t level,
                                   bool is_leaf,
                                   std::uint32_t parent_page_id,
                                   std::uint32_t right_sibling_page_id,
                                   std::span<const IndexBtreeSlotEntry> slots,
                                   std::span<const std::byte> payload)
{
    const auto slot_bytes = slots.size() * sizeof(IndexBtreeSlotEntry);
    if (slots.size() > std::numeric_limits<std::uint16_t>::max()) {
        return std::make_error_code(std::errc::value_too_large);
    }

    if (kIndexPayloadBase > kPageSize) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (kIndexPayloadBase + payload.size() > kPageSize) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (slot_bytes > kPageSize - (kIndexPayloadBase + payload.size())) {
        return std::make_error_code(std::errc::no_buffer_space);
    }

    if (!initialize_page(page, PageType::Index, page_id, wal_header.lsn, fsm)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto& header = page_header(page);
    auto& index_header = *reinterpret_cast<IndexBtreePageHeader*>(page.data() + kIndexHeaderOffset);

    index_header.magic = kIndexPageMagic;
    index_header.version = kIndexPageVersion;
    index_header.flags = 0U;
    if (is_leaf) {
        index_header.flags |= kIndexPageFlagLeaf;
    }
    index_header.level = level;
    index_header.parent_page_id = parent_page_id;
    index_header.right_sibling_page_id = right_sibling_page_id;

    if (!payload.empty()) {
        std::memcpy(page.data() + kIndexPayloadBase, payload.data(), payload.size());
    }

    const auto slot_start = kPageSize - slot_bytes;
    if (!slots.empty()) {
        std::memcpy(page.data() + slot_start, slots.data(), slot_bytes);
    }

    if (slot_start > kIndexPayloadBase + payload.size()) {
        const auto gap = slot_start - (kIndexPayloadBase + payload.size());
        std::memset(page.data() + kIndexPayloadBase + payload.size(), 0, gap);
    }

    header.flags |= static_cast<std::uint16_t>(PageFlag::Dirty);
    header.lsn = wal_header.lsn;
    header.free_start = static_cast<std::uint16_t>(kIndexPayloadBase + payload.size());
    header.free_end = static_cast<std::uint16_t>(slot_start);
    header.tuple_count = static_cast<std::uint16_t>(slots.size());
    header.fragment_count = 0U;

    if (fsm != nullptr) {
        sync_free_space(*fsm, as_const_span(page));
    }

    return {};
}

std::error_code read_internal_entries(std::span<const std::byte> page, std::vector<IndexInternalEntry>& out_entries)
{
    out_entries.clear();

    const auto& header = page_header(page);
    if (!is_valid(header) || static_cast<PageType>(header.type) != PageType::Index) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto& index = *reinterpret_cast<const IndexBtreePageHeader*>(page.data() + kIndexHeaderOffset);
    if (index_page_is_leaf(index)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto slot_count = static_cast<std::size_t>(header.tuple_count);
    if (slot_count == 0U) {
        return {};
    }

    const auto pointer_size = sizeof(IndexBtreeChildPointer);
    const auto slot_ptr = reinterpret_cast<const IndexBtreeSlotEntry*>(page.data() + header.free_end);

    out_entries.reserve(slot_count);

    for (std::size_t index_slot = 0; index_slot < slot_count; ++index_slot) {
        const auto& slot = slot_ptr[index_slot];
        if (slot.key_offset < kIndexPayloadBase || slot.key_offset > page.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (slot.key_offset < pointer_size) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto pointer_offset = static_cast<std::size_t>(slot.key_offset) - pointer_size;
        if (pointer_offset < kIndexPayloadBase || pointer_offset + pointer_size > static_cast<std::size_t>(header.free_start)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        IndexInternalEntry entry{};
        std::memcpy(&entry.child, page.data() + pointer_offset, pointer_size);

        const auto key_length = static_cast<std::size_t>(slot.effective_length());
        if (key_length != 0U) {
            if (static_cast<std::size_t>(slot.key_offset) + key_length > static_cast<std::size_t>(header.free_start)) {
                return std::make_error_code(std::errc::invalid_argument);
            }
            entry.key.resize(key_length);
            std::memcpy(entry.key.data(), page.data() + slot.key_offset, key_length);
        }

        entry.infinite = slot.infinite_key();
        out_entries.push_back(std::move(entry));
    }

    return {};
}

std::error_code write_internal_entries(std::span<std::byte> page,
                                       FreeSpaceMap* fsm,
                                       const WalRecordHeader& wal_header,
                                       std::uint32_t page_id,
                                       std::uint16_t level,
                                       std::uint32_t parent_page_id,
                                       std::uint32_t right_sibling_page_id,
                                       std::span<const IndexInternalEntry> entries)
{
    const auto pointer_size = sizeof(IndexBtreeChildPointer);
    std::size_t payload_bytes = 0U;
    for (const auto& entry : entries) {
        payload_bytes += pointer_size + entry.key.size();
    }

    std::vector<std::byte> payload(payload_bytes);
    std::vector<IndexBtreeSlotEntry> slots(entries.size());

    std::size_t cursor = 0U;
    for (std::size_t i = 0; i < entries.size(); ++i) {
        const auto& entry = entries[i];
        std::memcpy(payload.data() + cursor, &entry.child, pointer_size);
        cursor += pointer_size;

        const auto key_offset = cursor;
        if (!entry.key.empty()) {
            std::memcpy(payload.data() + cursor, entry.key.data(), entry.key.size());
            cursor += entry.key.size();
        }

        slots[i].key_offset = static_cast<std::uint16_t>(kIndexPayloadBase + key_offset);

        std::uint16_t key_length = static_cast<std::uint16_t>(entry.key.size());
        if (entry.infinite) {
            key_length |= kIndexBtreeSlotInfiniteKeyMask;
        }
        slots[i].key_length = key_length;
    }

    return rebuild_index_page(page,
                              fsm,
                              wal_header,
                              page_id,
                              level,
                              false,
                              parent_page_id,
                              right_sibling_page_id,
                              std::span<const IndexBtreeSlotEntry>(slots.data(), slots.size()),
                              std::span<const std::byte>(payload.data(), payload.size()));
}

std::error_code apply_index_split_parent(std::span<std::byte> page,
                                         FreeSpaceMap* fsm,
                                         const WalRecordHeader& wal_header,
                                         const WalIndexSplitView& view)
{
    std::vector<IndexInternalEntry> entries;

    auto parent_view = std::span<const std::byte>(page.data(), page.size());
    auto& header = page_header(parent_view);
    std::uint32_t parent_parent_page_id = 0U;
    std::uint32_t parent_right_sibling = 0U;
    bool existing_internal = false;

    if (is_valid(header) && static_cast<PageType>(header.type) == PageType::Index) {
        const auto& index_header = *reinterpret_cast<const IndexBtreePageHeader*>(parent_view.data() + kIndexHeaderOffset);
        parent_parent_page_id = index_header.parent_page_id;
        parent_right_sibling = index_header.right_sibling_page_id;
        if (!index_page_is_leaf(index_header) && header.tuple_count != 0U) {
            if (auto ec = read_internal_entries(parent_view, entries); ec) {
                return ec;
            }
            existing_internal = true;
        } else if (!index_page_is_leaf(index_header) && header.tuple_count == 0U) {
            existing_internal = true;
        }
    }

    const auto parent_level = static_cast<std::uint16_t>(view.header.level + 1U);

    if (entries.empty() && !existing_internal) {
        if (view.pivot_key.empty()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        IndexInternalEntry left_entry{};
        left_entry.child.page_id = view.header.left_page_id;
        left_entry.key.assign(view.pivot_key.begin(), view.pivot_key.end());
        left_entry.infinite = false;

        IndexInternalEntry sentinel{};
        sentinel.child.page_id = view.header.right_page_id;
        sentinel.infinite = true;

        entries.push_back(std::move(left_entry));
        entries.push_back(std::move(sentinel));

        return write_internal_entries(page,
                                      fsm,
                                      wal_header,
                                      view.header.parent_page_id,
                                      parent_level,
                                      parent_parent_page_id,
                                      parent_right_sibling,
                                      std::span<const IndexInternalEntry>(entries.data(), entries.size()));
    }

    if (view.header.parent_insert_slot >= entries.size()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto keys_equal = [&](const std::vector<std::byte>& lhs, std::span<const std::byte> rhs) {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        return std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
    };

    auto& target = entries[view.header.parent_insert_slot];

    const bool already_applied = !view.pivot_key.empty()
        && target.child.page_id == view.header.left_page_id
        && !target.infinite
        && keys_equal(target.key, view.pivot_key)
        && (view.header.parent_insert_slot + 1U) < entries.size()
        && entries[view.header.parent_insert_slot + 1U].child.page_id == view.header.right_page_id;

    if (already_applied) {
        bool sentinel_present = false;
        for (const auto& entry : entries) {
            if (entry.infinite) {
                sentinel_present = true;
                break;
            }
        }
        if (sentinel_present) {
            return {};
        }
    }

    const auto displaced_key = target.key;
    const bool displaced_infinite = target.infinite;

    if (view.pivot_key.empty()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    target.child.page_id = view.header.left_page_id;
    target.key.assign(view.pivot_key.begin(), view.pivot_key.end());
    target.infinite = false;

    IndexInternalEntry new_entry{};
    new_entry.child.page_id = view.header.right_page_id;
    new_entry.key = displaced_key;
    new_entry.infinite = displaced_infinite;

    entries.insert(entries.begin() + static_cast<std::ptrdiff_t>(view.header.parent_insert_slot + 1U), std::move(new_entry));

    return write_internal_entries(page,
                                  fsm,
                                  wal_header,
                                  view.header.parent_page_id,
                                  parent_level,
                                  parent_parent_page_id,
                                  parent_right_sibling,
                                  std::span<const IndexInternalEntry>(entries.data(), entries.size()));
}

std::error_code apply_index_split(WalReplayContext& context, const WalRecoveryRecord& record)
{
    auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());
    auto view = decode_wal_index_split(payload);
    if (!view) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto split_flags = static_cast<WalIndexSplitFlag>(view->header.flags);
    const bool is_leaf_split = has_flag(split_flags, WalIndexSplitFlag::Leaf);

    if (view->header.left_page_id == 0U || view->header.right_page_id == 0U) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto fsm = context.free_space_map();

    {
        auto left_page = context.get_page(view->header.left_page_id);
        if (auto ec = rebuild_index_page(left_page,
                                         fsm,
                                         record.header,
                                         view->header.left_page_id,
                                         view->header.level,
                                         is_leaf_split,
                                         view->header.parent_page_id,
                                         view->header.right_page_id,
                                         view->left_slots,
                                         view->left_payload);
            ec) {
            return ec;
        }
    }

    {
        auto right_page = context.get_page(view->header.right_page_id);
        if (auto ec = rebuild_index_page(right_page,
                                         fsm,
                                         record.header,
                                         view->header.right_page_id,
                                         view->header.level,
                                         is_leaf_split,
                                         view->header.parent_page_id,
                                         view->header.right_sibling_page_id,
                                         view->right_slots,
                                         view->right_payload);
            ec) {
            return ec;
        }
    }

    if (view->header.parent_page_id != 0U) {
        auto parent_page = context.get_page(view->header.parent_page_id);
        if (auto ec = apply_index_split_parent(parent_page, fsm, record.header, *view); ec) {
            return ec;
        }
    }

    return {};
}

}  // namespace

WalReplayContext::WalReplayContext(PageType default_page_type, FreeSpaceMap* fsm)
    : default_page_type_{default_page_type}
    , free_space_map_{fsm}
{
}

void WalReplayContext::set_page(std::uint32_t page_id, std::span<const std::byte> image)
{
    if (image.size() != kPageSize) {
        throw std::invalid_argument{"Page image must match kPageSize"};
    }
    auto& slot = pages_[page_id];
    std::copy(image.begin(), image.end(), slot.begin());
    if (free_space_map_) {
        sync_free_space(*free_space_map_, std::span<const std::byte>(slot.data(), slot.size()));
    }
}

std::span<std::byte> WalReplayContext::get_page(std::uint32_t page_id)
{
    auto [it, inserted] = pages_.try_emplace(page_id);
    if (inserted) {
        std::fill(it->second.begin(), it->second.end(), std::byte{0});
        auto span = std::span<std::byte>(it->second.data(), it->second.size());
        if (!initialize_page(span, default_page_type_, page_id, 0U, free_space_map_)) {
            throw std::runtime_error{"Failed to initialise replay page"};
        }
        return span;
    }
    if (free_space_map_) {
        sync_free_space(*free_space_map_, std::span<const std::byte>(it->second.data(), it->second.size()));
    }
    return {it->second.data(), it->second.size()};
}

void WalReplayContext::set_free_space_map(FreeSpaceMap* fsm) noexcept
{
    free_space_map_ = fsm;
}

FreeSpaceMap* WalReplayContext::free_space_map() const noexcept
{
    return free_space_map_;
}

void WalReplayContext::set_checkpoint_index_metadata(std::span<const CheckpointIndexMetadata> metadata)
{
    checkpoint_index_metadata_.assign(metadata.begin(), metadata.end());
}

const std::vector<CheckpointIndexMetadata>& WalReplayContext::checkpoint_index_metadata() const noexcept
{
    return checkpoint_index_metadata_;
}

void WalReplayContext::record_index_metadata(std::span<const WalCompactionEntry> entries)
{
    index_metadata_events_.insert(index_metadata_events_.end(), entries.begin(), entries.end());
}

const std::vector<WalCompactionEntry>& WalReplayContext::index_metadata() const noexcept
{
    return index_metadata_events_;
}

WalReplayer::WalReplayer(WalReplayContext& context)
    : context_{context}
{
}

std::error_code WalReplayer::apply_redo(const WalRecoveryPlan& plan)
{
    auto telemetry_state = plan.telemetry;
    const auto redo_start = std::chrono::steady_clock::now();
    auto record_redo = [&](std::error_code ec) -> std::error_code {
        if (telemetry_state) {
            const auto redo_end = std::chrono::steady_clock::now();
            const auto redo_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(redo_end - redo_start);
            const auto duration_ns = redo_ns.count() > 0 ? static_cast<std::uint64_t>(redo_ns.count()) : 0ULL;
            telemetry_state->record_redo(duration_ns, !ec);
        }
        return ec;
    };

    context_.set_checkpoint_index_metadata(std::span<const CheckpointIndexMetadata>(plan.checkpoint_index_metadata.data(),
                                                                                    plan.checkpoint_index_metadata.size()));

    for (const auto& snapshot : plan.checkpoint_page_snapshots) {
        if (snapshot.entry.page_id == 0U) {
            return record_redo(std::make_error_code(std::errc::invalid_argument));
        }
        if (snapshot.image.size() != kPageSize) {
            return record_redo(std::make_error_code(std::errc::invalid_argument));
        }
        context_.set_page(snapshot.entry.page_id,
                          std::span<const std::byte>(snapshot.image.data(), snapshot.image.size()));
    }

    for (const auto& record : plan.redo) {
        if (auto ec = apply_redo_record(record); ec) {
            return record_redo(ec);
        }
    }
    return record_redo({});
}

std::error_code WalReplayer::apply_undo(const WalRecoveryPlan& plan)
{
    auto telemetry_state = plan.telemetry;
    const auto undo_start = std::chrono::steady_clock::now();
    bool undo_recorded = false;
    auto record_undo = [&](std::error_code ec) -> std::error_code {
        if (!undo_recorded && telemetry_state) {
            const auto undo_end = std::chrono::steady_clock::now();
            const auto undo_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(undo_end - undo_start);
            const auto duration_ns = undo_ns.count() > 0 ? static_cast<std::uint64_t>(undo_ns.count()) : 0ULL;
            telemetry_state->record_undo(duration_ns, !ec);
            undo_recorded = true;
        }
        return ec;
    };

    if (plan.undo_spans.empty()) {
        for (const auto& record : plan.undo) {
            last_undo_type_ = static_cast<WalRecordType>(record.header.type);
            if (auto ec = apply_undo_record(record); ec) {
                return record_undo(ec);
            }
        }
        last_undo_type_.reset();
        (void)record_undo({});
        return run_temp_cleanup(plan);
    }

    WalUndoWalker walker{plan};

    while (auto work_item = walker.next()) {
        if (work_item->owner_page_id != 0U) {
            PageType owner_type = PageType::Table;
            {
                auto existing_page = context_.get_page(work_item->owner_page_id);
                auto existing_header = page_header(std::span<const std::byte>(existing_page.data(), existing_page.size()));
                if (is_valid(existing_header)) {
                    owner_type = static_cast<PageType>(existing_header.type);
                }
            }
            (void)ensure_page(context_, work_item->owner_page_id, owner_type);
        }
        for (auto overflow_page_id : work_item->overflow_page_ids) {
            if (overflow_page_id != 0U) {
                (void)ensure_page(context_, overflow_page_id, PageType::Overflow);
            }
        }

        for (const auto& record : work_item->records) {
            last_undo_type_ = static_cast<WalRecordType>(record.header.type);
            if (auto ec = apply_undo_record(record); ec) {
                return record_undo(ec);
            }
        }
    }

    last_undo_type_.reset();
    (void)record_undo({});
    return run_temp_cleanup(plan);
}

std::error_code WalReplayer::apply_redo_record(const WalRecoveryRecord& record)
{
    auto fsm = context_.free_space_map();
    auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());

    const auto record_type = static_cast<WalRecordType>(record.header.type);

    switch (record_type) {
    case WalRecordType::TupleInsert:
    case WalRecordType::CatalogInsert: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto tuple_payload = wal_tuple_payload(payload, *meta);
        if (tuple_payload.size() != meta->tuple_length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
    auto page = ensure_page(context_, record.header.page_id, replay_page_type(record_type));
    return wal::apply_tuple_insert(page, record.header, *meta, tuple_payload, fsm, false);
    }
    case WalRecordType::TupleDelete:
    case WalRecordType::CatalogDelete: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
    auto page = ensure_page(context_, record.header.page_id, replay_page_type(record_type));
    return wal::apply_tuple_delete(page, record.header, *meta, fsm, false);
    }
    case WalRecordType::TupleUpdate:
    case WalRecordType::CatalogUpdate: {
        auto meta = decode_wal_tuple_update_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto tuple_payload = wal_tuple_update_payload(payload, *meta);
        if (tuple_payload.size() != meta->base.tuple_length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
    auto page = ensure_page(context_, record.header.page_id, replay_page_type(record_type));
    return wal::apply_tuple_update(page, record.header, *meta, tuple_payload, fsm);
    }
    case WalRecordType::TupleBeforeImage:
        return {};
    case WalRecordType::TupleOverflowChunk: {
        auto meta = decode_wal_overflow_chunk_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto chunk_payload = wal_overflow_chunk_payload(payload, *meta);
        if (chunk_payload.size() != meta->chunk_length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        return apply_overflow_chunk(context_, record, *meta, chunk_payload);
    }
    case WalRecordType::TupleOverflowTruncate: {
        auto meta = decode_wal_overflow_truncate_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        return apply_overflow_truncate(context_, record.header, *meta);
    }
    case WalRecordType::PageCompaction: {
        auto view = decode_wal_compaction(payload);
        if (!view) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto page = ensure_page(context_, record.header.page_id, PageType::Table);
        bool applied = false;
        if (auto ec = apply_page_compaction(page, record.header, *view, fsm, applied); ec) {
            return ec;
        }
        if (applied) {
            context_.record_index_metadata(std::span<const WalCompactionEntry>(view->entries.data(), view->entries.size()));
        }
        return {};
    }
    case WalRecordType::IndexSplit:
        return apply_index_split(context_, record);
    case WalRecordType::IndexMerge:
        return std::make_error_code(std::errc::not_supported);
    case WalRecordType::IndexBulkCheckpoint:
        return {};
    default:
        return std::make_error_code(std::errc::not_supported);
    }
}

std::error_code WalReplayer::apply_undo_record(const WalRecoveryRecord& record)
{
    auto fsm = context_.free_space_map();
    auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());

    const auto record_type = static_cast<WalRecordType>(record.header.type);

    switch (record_type) {
    case WalRecordType::TupleInsert:
    case WalRecordType::CatalogInsert: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
    auto page = ensure_page(context_, record.header.page_id, replay_page_type(record_type));
    return wal::apply_tuple_delete(page, record.header, *meta, fsm, true);
    }
    case WalRecordType::TupleUpdate:
    case WalRecordType::CatalogUpdate: {
        auto meta = decode_wal_tuple_update_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
    auto page = ensure_page(context_, record.header.page_id, replay_page_type(record_type));
    return wal::apply_tuple_delete(page, record.header, meta->base, fsm, false);
    }
    case WalRecordType::TupleBeforeImage: {
        auto before_view = decode_wal_tuple_before_image(payload);
        if (!before_view) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (before_view->tuple_payload.size() != before_view->meta.tuple_length) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        PageType target_type = PageType::Table;
        {
            auto existing_page = context_.get_page(record.header.page_id);
            auto existing_header = page_header(std::span<const std::byte>(existing_page.data(), existing_page.size()));
            if (is_valid(existing_header)) {
                target_type = static_cast<PageType>(existing_header.type);
            }
        }

        auto page = ensure_page(context_, record.header.page_id, target_type);
        if (auto ec = wal::apply_tuple_insert(page,
                                              record.header,
                                              before_view->meta,
                                              before_view->tuple_payload,
                                              fsm,
                                              true,
                                              before_view->previous_page_lsn,
                                              before_view->previous_free_start,
                                              before_view->previous_tuple_offset);
            ec) {
            return ec;
        }

        for (const auto& chunk_view : before_view->overflow_chunks) {
            auto chunk_page = ensure_page(context_, chunk_view.meta.overflow_page_id, PageType::Overflow);
            if (!write_overflow_chunk(chunk_page,
                                      chunk_view.meta,
                                      chunk_view.payload,
                                      record.header.lsn,
                                      context_.free_space_map())) {
                return std::make_error_code(std::errc::io_error);
            }
        }

        return {};
    }
    case WalRecordType::TupleDelete:
    case WalRecordType::CatalogDelete:
        return {};
    case WalRecordType::TupleOverflowChunk: {
        auto meta = decode_wal_overflow_chunk_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        return undo_overflow_chunk(context_, record, *meta);
    }
    case WalRecordType::TupleOverflowTruncate: {
        auto meta = decode_wal_overflow_truncate_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        return undo_overflow_truncate(context_, record, *meta);
    }
    case WalRecordType::PageCompaction: {
        auto view = decode_wal_compaction(payload);
        if (!view) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto page = ensure_page(context_, record.header.page_id, PageType::Table);
        if (auto ec = undo_page_compaction(page, record.header, *view, fsm); ec) {
            return ec;
        }
        context_.record_index_metadata(std::span<const WalCompactionEntry>(view->entries.data(), view->entries.size()));
        return {};
    }
    case WalRecordType::IndexSplit:
    case WalRecordType::IndexMerge:
    case WalRecordType::IndexBulkCheckpoint:
        return std::make_error_code(std::errc::not_supported);
    default:
        return std::make_error_code(std::errc::not_supported);
    }
}

std::optional<WalRecordType> WalReplayer::last_undo_type() const noexcept
{
    return last_undo_type_;
}

std::error_code WalReplayer::run_temp_cleanup(const WalRecoveryPlan& plan) const
{
    auto telemetry_state = plan.telemetry;
    const auto cleanup_start = std::chrono::steady_clock::now();
    auto record_cleanup = [&](std::error_code ec) -> std::error_code {
        if (telemetry_state) {
            const auto cleanup_end = std::chrono::steady_clock::now();
            const auto cleanup_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(cleanup_end - cleanup_start);
            const auto duration_ns = cleanup_ns.count() > 0 ? static_cast<std::uint64_t>(cleanup_ns.count()) : 0ULL;
            telemetry_state->record_cleanup(duration_ns, !ec);
        }
        return ec;
    };

    if (plan.temp_resource_registry == nullptr) {
        return record_cleanup({});
    }

    TempResourcePurgeStats stats{};
    return record_cleanup(plan.temp_resource_registry->purge(TempResourcePurgeReason::Recovery, &stats));
}

}  // namespace bored::storage
