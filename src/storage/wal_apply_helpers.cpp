#include "bored/storage/wal_apply_helpers.hpp"

#include "bored/storage/free_space_map.hpp"

#include <algorithm>
#include <cstring>

namespace bored::storage::wal {

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

}  // namespace

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
                                   bool force,
                                   std::uint64_t forced_lsn,
                                   std::uint16_t forced_free_start,
                                   std::uint16_t forced_tuple_offset)
{
    if (page_header(page).page_id != meta.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (payload.size() <= tuple_header_size()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    TupleHeader tuple_header{};
    std::memcpy(&tuple_header, payload.data(), tuple_header_size());
    auto tuple_payload = payload.subspan(tuple_header_size());
    const bool inserted_overflow = is_overflow_tuple(tuple_payload);
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
        const auto previous_free_start = header_ref.free_start;
        const auto previous_offset = slot.offset;
        const auto previous_length = slot.length;
        if (previous_length > 0U) {
            if (static_cast<std::size_t>(previous_offset) + previous_length > page.size()) {
                return std::make_error_code(std::errc::invalid_argument);
            }
            std::memset(page.data() + previous_offset, 0, previous_length);
        }

        const auto target_offset = (forced_tuple_offset != 0U) ? forced_tuple_offset : previous_offset;
        if (static_cast<std::size_t>(target_offset) + payload.size() > page.size()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::memcpy(page.data() + target_offset, payload.data(), payload.size());
        slot.offset = target_offset;
        slot.length = static_cast<std::uint16_t>(payload.size());

        const auto tuple_end = static_cast<std::uint16_t>(target_offset + slot.length);
        if (previous_free_start > tuple_end) {
            std::memset(page.data() + tuple_end, 0, previous_free_start - tuple_end);
        }

        if (previous_length == 0U && header_ref.fragment_count > 0U) {
            --header_ref.fragment_count;
        }

        auto desired_free_start = (forced_free_start != 0U) ? forced_free_start : previous_free_start;
        if (desired_free_start < tuple_end) {
            desired_free_start = tuple_end;
        }
        for (const auto& other_slot : directory) {
            if (other_slot.length == 0U) {
                continue;
            }
            const auto other_end = static_cast<std::uint16_t>(other_slot.offset + other_slot.length);
            if (desired_free_start < other_end) {
                desired_free_start = other_end;
            }
        }
        if (desired_free_start > previous_free_start) {
            std::memset(page.data() + previous_free_start, 0, desired_free_start - previous_free_start);
        }

        header_ref.flags |= static_cast<std::uint16_t>(PageFlag::Dirty);
        const auto restored_lsn = forced_lsn != 0U ? forced_lsn : (header.prev_lsn != 0U ? header.prev_lsn : previous_lsn);
        header_ref.lsn = restored_lsn;
        header_ref.free_start = desired_free_start;
        if (inserted_overflow) {
            set_has_overflow(header_ref, true);
        } else {
            refresh_overflow_flag(page);
        }
        if (fsm != nullptr) {
            sync_free_space(*fsm, page);
        }
        if (header_ref.free_start < header_ref.free_end) {
            std::memset(page.data() + header_ref.free_start,
                        0,
                        static_cast<std::size_t>(header_ref.free_end) - header_ref.free_start);
        }
        return {};
    }

    if (page_already_applied(page, header)) {
        return {};
    }

    auto appended = append_tuple(page, tuple_header, tuple_payload, header.lsn, fsm);
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
                                   FreeSpaceMap* fsm,
                                   bool force)
{
    if (page_header(page).page_id != meta.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (!force && page_already_applied(page, header)) {
        return {};
    }

    auto tuple_view = read_tuple(as_const_span(page), meta.slot_index);
    if (tuple_view.empty()) {
        return {};
    }

    const auto delete_lsn = (force && header.prev_lsn != 0U) ? header.prev_lsn : header.lsn;

    if (!delete_tuple(page, meta.slot_index, delete_lsn, fsm)) {
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

    if (payload.size() <= tuple_header_size()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    TupleHeader tuple_header{};
    std::memcpy(&tuple_header, payload.data(), tuple_header_size());
    auto tuple_payload = payload.subspan(tuple_header_size());

    if (!delete_tuple(page, meta.base.slot_index, header.lsn, fsm)) {
        return std::make_error_code(std::errc::io_error);
    }

    auto appended = append_tuple(page, tuple_header, tuple_payload, header.lsn, fsm);
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

    if (is_overflow_tuple(tuple_payload)) {
        auto& header_ref = page_header(page);
        set_has_overflow(header_ref, true);
    } else {
        refresh_overflow_flag(page);
    }

    return {};
}

}  // namespace bored::storage::wal
