#include "bored/storage/wal_replayer.hpp"

#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace bored::storage {

namespace {

std::span<const std::byte> as_const_span(std::span<std::byte> buffer)
{
    return {buffer.data(), buffer.size()};
}

void set_has_overflow(PageHeader& header, bool value)
{
    const auto flag_bit = static_cast<std::uint16_t>(PageFlag::HasOverflow);
    if (value) {
        header.flags |= flag_bit;
    } else {
        header.flags &= static_cast<std::uint16_t>(~flag_bit);
    }
}

bool page_contains_overflow(std::span<const std::byte> page)
{
    const auto& header = page_header(page);
    if (!is_valid(header) || header.tuple_count == 0U) {
        return false;
    }

    for (std::uint16_t index = 0U; index < header.tuple_count; ++index) {
        auto tuple = read_tuple(page, index);
        if (tuple.empty()) {
            continue;
        }
        if (is_overflow_tuple(tuple)) {
            return true;
        }
    }

    return false;
}

void refresh_overflow_flag(std::span<std::byte> page)
{
    auto const_view = as_const_span(page);
    auto& header = page_header(page);
    set_has_overflow(header, page_contains_overflow(const_view));
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

bool page_already_applied(std::span<const std::byte> page, const WalRecordHeader& header)
{
    const auto& current = page_header(page);
    if (!is_valid(current)) {
        return false;
    }
    return current.lsn >= header.lsn;
}

std::error_code apply_tuple_insert(std::span<std::byte> page,
                                   const WalRecordHeader& header,
                                   const WalTupleMeta& meta,
                                   std::span<const std::byte> payload,
                                   FreeSpaceMap* fsm,
                                   bool force)
{
    if (page_header(page).page_id != meta.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const bool inserted_overflow = is_overflow_tuple(payload);
    const auto previous_lsn = page_header(page).lsn;

    if (force) {
        auto& header_ref = page_header(page);
        if (payload.size() != meta.tuple_length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto directory = slot_directory(page);
        if (meta.slot_index >= directory.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto& slot = directory[meta.slot_index];
        if (slot.offset + payload.size() > page.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        std::memcpy(page.data() + slot.offset, payload.data(), payload.size());
        slot.length = static_cast<std::uint16_t>(payload.size());
        if (header_ref.fragment_count > 0U) {
            --header_ref.fragment_count;
        }
        header_ref.flags |= static_cast<std::uint16_t>(PageFlag::Dirty);
        header_ref.lsn = std::max(previous_lsn, header.lsn);
        if (inserted_overflow) {
            set_has_overflow(header_ref, true);
        } else {
            refresh_overflow_flag(page);
        }
        if (fsm != nullptr) {
            sync_free_space(*fsm, page);
        }
        return {};
    }

    if (page_already_applied(page, header)) {
        return {};
    }

    auto appended = append_tuple(page, payload, header.lsn, fsm);
    if (!appended) {
        return std::make_error_code(std::errc::io_error);
    }

    if (appended->index != meta.slot_index) {
        auto directory = slot_directory(page);
        if (meta.slot_index >= directory.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        std::swap(directory[appended->index], directory[meta.slot_index]);
    }

    if (inserted_overflow) {
        auto& header_ref = page_header(page);
        set_has_overflow(header_ref, true);
    } else {
        refresh_overflow_flag(page);
    }

    return {};
}

std::error_code apply_tuple_delete(std::span<std::byte> page,
                                   const WalRecordHeader& header,
                                   const WalTupleMeta& meta,
                                   FreeSpaceMap* fsm)
{
    if (page_header(page).page_id != meta.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (page_already_applied(page, header)) {
        return {};
    }

    if (!delete_tuple(page, meta.slot_index, header.lsn, fsm)) {
        return std::make_error_code(std::errc::io_error);
    }

    refresh_overflow_flag(page);

    return {};
}

std::error_code apply_tuple_update(std::span<std::byte> page,
                                   const WalRecordHeader& header,
                                   const WalTupleUpdateMeta& meta,
                                   std::span<const std::byte> payload,
                                   FreeSpaceMap* fsm)
{
    if (page_header(page).page_id != meta.base.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (page_already_applied(page, header)) {
        return {};
    }

    if (!delete_tuple(page, meta.base.slot_index, header.lsn, fsm)) {
        return std::make_error_code(std::errc::io_error);
    }

    auto appended = append_tuple(page, payload, header.lsn, fsm);
    if (!appended) {
        return std::make_error_code(std::errc::io_error);
    }

    if (appended->index != meta.base.slot_index) {
        auto directory = slot_directory(page);
        if (meta.base.slot_index >= directory.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        std::swap(directory[appended->index], directory[meta.base.slot_index]);
    }

    if (is_overflow_tuple(payload)) {
        auto& header_ref = page_header(page);
        set_has_overflow(header_ref, true);
    } else {
        refresh_overflow_flag(page);
    }

    return {};
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
    if (page_already_applied(page, record.header)) {
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

        if (!page_already_applied(page, header)) {
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
    for (const auto& record : plan.undo) {
        if (auto ec = apply_undo_record(record); ec) {
            return ec;
        }
    }
    return {};
}

std::error_code WalReplayer::apply_redo_record(const WalRecoveryRecord& record)
{
    auto fsm = context_.free_space_map();
    auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());

    switch (static_cast<WalRecordType>(record.header.type)) {
    case WalRecordType::TupleInsert: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto tuple_payload = wal_tuple_payload(payload, *meta);
        if (tuple_payload.size() != meta->tuple_length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto page = ensure_page(context_, record.header.page_id, PageType::Table);
        return apply_tuple_insert(page, record.header, *meta, tuple_payload, fsm, false);
    }
    case WalRecordType::TupleDelete: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto page = ensure_page(context_, record.header.page_id, PageType::Table);
        return apply_tuple_delete(page, record.header, *meta, fsm);
    }
    case WalRecordType::TupleUpdate: {
        auto meta = decode_wal_tuple_update_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto tuple_payload = wal_tuple_update_payload(payload, *meta);
        if (tuple_payload.size() != meta->base.tuple_length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto page = ensure_page(context_, record.header.page_id, PageType::Table);
        return apply_tuple_update(page, record.header, *meta, tuple_payload, fsm);
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
    default:
        return std::make_error_code(std::errc::not_supported);
    }
}

std::error_code WalReplayer::apply_undo_record(const WalRecoveryRecord& record)
{
    auto fsm = context_.free_space_map();
    auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());

    switch (static_cast<WalRecordType>(record.header.type)) {
    case WalRecordType::TupleInsert: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto page = ensure_page(context_, record.header.page_id, PageType::Table);
        return apply_tuple_delete(page, record.header, *meta, fsm);
    }
    case WalRecordType::TupleUpdate: {
        auto meta = decode_wal_tuple_update_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto page = ensure_page(context_, record.header.page_id, PageType::Table);
        return apply_tuple_delete(page, record.header, meta->base, fsm);
    }
    case WalRecordType::TupleBeforeImage: {
        auto before_view = decode_wal_tuple_before_image(payload);
        if (!before_view) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (before_view->tuple_payload.size() != before_view->meta.tuple_length) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        auto page = ensure_page(context_, record.header.page_id, PageType::Table);
        if (auto ec = apply_tuple_insert(page,
                                         record.header,
                                         before_view->meta,
                                         before_view->tuple_payload,
                                         fsm,
                                         true);
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
    default:
        return std::make_error_code(std::errc::not_supported);
    }
}

}  // namespace bored::storage
