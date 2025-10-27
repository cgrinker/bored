#include "bored/storage/wal_replayer.hpp"

#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_apply_helpers.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_undo_walker.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <vector>

namespace bored::storage {

namespace {

std::span<const std::byte> as_const_span(std::span<std::byte> buffer)
{
    return {buffer.data(), buffer.size()};
}

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
    for (const auto& record : plan.redo) {
        if (auto ec = apply_redo_record(record); ec) {
            return ec;
        }
    }
    return {};
}

std::error_code WalReplayer::apply_undo(const WalRecoveryPlan& plan)
{
    if (plan.undo_spans.empty()) {
        for (const auto& record : plan.undo) {
            last_undo_type_ = static_cast<WalRecordType>(record.header.type);
            if (auto ec = apply_undo_record(record); ec) {
                return ec;
            }
        }
        last_undo_type_.reset();
        return {};
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
                return ec;
            }
        }
    }

    last_undo_type_.reset();
    return {};
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
    default:
        return std::make_error_code(std::errc::not_supported);
    }
}

std::optional<WalRecordType> WalReplayer::last_undo_type() const noexcept
{
    return last_undo_type_;
}

}  // namespace bored::storage
