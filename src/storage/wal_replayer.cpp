#include "bored/storage/wal_replayer.hpp"

#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <algorithm>
#include <stdexcept>
#include <system_error>

namespace bored::storage {

namespace {

std::span<std::byte> ensure_page(WalReplayContext& context, std::uint32_t page_id)
{
    return context.get_page(page_id);
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
                                   std::span<const std::byte> payload)
{
    if (page_header(page).page_id != meta.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (page_already_applied(page, header)) {
        return {};
    }

    auto appended = append_tuple(page, payload, header.lsn, nullptr);
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

    return {};
}

std::error_code apply_tuple_delete(std::span<std::byte> page,
                                   const WalRecordHeader& header,
                                   const WalTupleMeta& meta)
{
    if (page_header(page).page_id != meta.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (page_already_applied(page, header)) {
        return {};
    }

    if (!delete_tuple(page, meta.slot_index, header.lsn, nullptr)) {
        return std::make_error_code(std::errc::io_error);
    }

    return {};
}

std::error_code apply_tuple_update(std::span<std::byte> page,
                                   const WalRecordHeader& header,
                                   const WalTupleUpdateMeta& meta,
                                   std::span<const std::byte> payload)
{
    if (page_header(page).page_id != meta.base.page_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (page_already_applied(page, header)) {
        return {};
    }

    if (!delete_tuple(page, meta.base.slot_index, header.lsn, nullptr)) {
        return std::make_error_code(std::errc::io_error);
    }

    auto appended = append_tuple(page, payload, header.lsn, nullptr);
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

    return {};
}

}  // namespace

WalReplayContext::WalReplayContext(PageType default_page_type)
    : default_page_type_{default_page_type}
{
}

void WalReplayContext::set_page(std::uint32_t page_id, std::span<const std::byte> image)
{
    if (image.size() != kPageSize) {
        throw std::invalid_argument{"Page image must match kPageSize"};
    }
    auto& slot = pages_[page_id];
    std::copy(image.begin(), image.end(), slot.begin());
}

std::span<std::byte> WalReplayContext::get_page(std::uint32_t page_id)
{
    auto [it, inserted] = pages_.try_emplace(page_id);
    if (inserted) {
        std::fill(it->second.begin(), it->second.end(), std::byte{0});
        auto span = std::span<std::byte>(it->second.data(), it->second.size());
        if (!initialize_page(span, default_page_type_, page_id, 0U, nullptr)) {
            throw std::runtime_error{"Failed to initialise replay page"};
        }
        return span;
    }
    return {it->second.data(), it->second.size()};
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
    auto page = ensure_page(context_, record.header.page_id);
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
        return apply_tuple_insert(page, record.header, *meta, tuple_payload);
    }
    case WalRecordType::TupleDelete: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        return apply_tuple_delete(page, record.header, *meta);
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
        return apply_tuple_update(page, record.header, *meta, tuple_payload);
    }
    default:
        return std::make_error_code(std::errc::not_supported);
    }
}

std::error_code WalReplayer::apply_undo_record(const WalRecoveryRecord& record)
{
    auto page = ensure_page(context_, record.header.page_id);
    auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());

    switch (static_cast<WalRecordType>(record.header.type)) {
    case WalRecordType::TupleInsert: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        return apply_tuple_delete(page, record.header, *meta);
    }
    default:
        return std::make_error_code(std::errc::not_supported);
    }
}

}  // namespace bored::storage
