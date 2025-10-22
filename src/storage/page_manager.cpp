#include "bored/storage/page_manager.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <vector>

namespace bored::storage {

namespace {

std::span<const std::byte> as_const_span(std::span<std::byte> buffer)
{
    return {buffer.data(), buffer.size()};
}

}  // namespace

PageManager::PageManager(FreeSpaceMap* fsm, std::shared_ptr<WalWriter> wal_writer)
    : fsm_{fsm}
    , wal_writer_{std::move(wal_writer)}
{
    if (!wal_writer_) {
        throw std::invalid_argument{"PageManager requires a WalWriter instance"};
    }
}

std::error_code PageManager::initialize_page(std::span<std::byte> page,
                                             PageType type,
                                             std::uint32_t page_id,
                                             std::uint64_t base_lsn) const
{
    if (!bored::storage::initialize_page(page, type, page_id, base_lsn, fsm_)) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    return {};
}

std::error_code PageManager::insert_tuple(std::span<std::byte> page,
                                          std::span<const std::byte> payload,
                                          std::uint64_t row_id,
                                          TupleInsertResult& out_result) const
{
    if (payload.empty()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto plan = prepare_append_tuple(as_const_span(page), payload.size());
    if (!plan) {
        return std::make_error_code(std::errc::no_buffer_space);
    }

    const auto& header = page_header(as_const_span(page));

    WalTupleMeta meta{};
    meta.page_id = header.page_id;
    meta.slot_index = plan->slot_index;
    meta.tuple_length = plan->tuple_length;
    meta.row_id = row_id;

    std::vector<std::byte> wal_buffer(wal_tuple_insert_payload_size(meta.tuple_length));
    auto wal_buffer_span = std::span<std::byte>(wal_buffer.data(), wal_buffer.size());
    if (!encode_wal_tuple_insert(wal_buffer_span, meta, payload)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::TupleInsert;
    descriptor.page_id = header.page_id;
    descriptor.flags = WalRecordFlag::None;
    descriptor.payload = std::span<const std::byte>(wal_buffer_span.data(), wal_buffer_span.size());

    WalAppendResult wal_result{};
    if (auto ec = wal_writer_->append_record(descriptor, wal_result); ec) {
        return ec;
    }

    auto slot = append_tuple(page, payload, wal_result.lsn, fsm_);
    if (!slot) {
        return std::make_error_code(std::errc::io_error);
    }

    out_result.slot = *slot;
    out_result.wal = wal_result;
    return {};
}

std::error_code PageManager::delete_tuple(std::span<std::byte> page,
                                          std::uint16_t slot_index,
                                          std::uint64_t row_id,
                                          TupleDeleteResult& out_result) const
{
    auto tuple_view = read_tuple(as_const_span(page), slot_index);
    if (tuple_view.empty()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto& header = page_header(as_const_span(page));

    WalTupleMeta meta{};
    meta.page_id = header.page_id;
    meta.slot_index = slot_index;
    meta.tuple_length = 0U;
    meta.row_id = row_id;

    std::vector<std::byte> wal_buffer(wal_tuple_delete_payload_size());
    auto wal_buffer_span = std::span<std::byte>(wal_buffer.data(), wal_buffer.size());
    if (!encode_wal_tuple_delete(wal_buffer_span, meta)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::TupleDelete;
    descriptor.page_id = header.page_id;
    descriptor.flags = WalRecordFlag::None;
    descriptor.payload = std::span<const std::byte>(wal_buffer_span.data(), wal_buffer_span.size());

    WalAppendResult wal_result{};
    if (auto ec = wal_writer_->append_record(descriptor, wal_result); ec) {
        return ec;
    }

    if (!bored::storage::delete_tuple(page, slot_index, wal_result.lsn, fsm_)) {
        return std::make_error_code(std::errc::io_error);
    }

    out_result.wal = wal_result;
    return {};
}

std::error_code PageManager::update_tuple(std::span<std::byte> page,
                                          std::uint16_t slot_index,
                                          std::span<const std::byte> new_payload,
                                          std::uint64_t row_id,
                                          TupleUpdateResult& out_result) const
{
    if (new_payload.empty()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto page_const = as_const_span(page);
    auto current_tuple = read_tuple(page_const, slot_index);
    if (current_tuple.empty()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto& header = page_header(page_const);
    if (new_payload.size() > std::numeric_limits<std::uint16_t>::max()) {
        return std::make_error_code(std::errc::value_too_large);
    }

    const auto old_length = static_cast<std::uint16_t>(current_tuple.size());
    const auto new_length = static_cast<std::uint16_t>(new_payload.size());

    if (new_length > old_length) {
        const auto free_bytes = compute_free_bytes(header);
        const auto extra_required = static_cast<std::size_t>(new_length) - static_cast<std::size_t>(old_length);
        if (free_bytes < extra_required) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
    }

    WalTupleUpdateMeta meta{};
    meta.base.page_id = header.page_id;
    meta.base.slot_index = slot_index;
    meta.base.tuple_length = new_length;
    meta.base.row_id = row_id;
    meta.old_length = old_length;

    std::vector<std::byte> wal_buffer(wal_tuple_update_payload_size(meta.base.tuple_length));
    auto wal_buffer_span = std::span<std::byte>(wal_buffer.data(), wal_buffer.size());
    if (!encode_wal_tuple_update(wal_buffer_span, meta, new_payload)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::TupleUpdate;
    descriptor.page_id = header.page_id;
    descriptor.flags = WalRecordFlag::None;
    descriptor.payload = std::span<const std::byte>(wal_buffer_span.data(), wal_buffer_span.size());

    WalAppendResult wal_result{};
    if (auto ec = wal_writer_->append_record(descriptor, wal_result); ec) {
        return ec;
    }

    if (!bored::storage::delete_tuple(page, slot_index, wal_result.lsn, fsm_)) {
        return std::make_error_code(std::errc::io_error);
    }

    auto appended = append_tuple(page, new_payload, wal_result.lsn, fsm_);
    if (!appended) {
        return std::make_error_code(std::errc::io_error);
    }

    auto new_slot = *appended;
    if (new_slot.index != slot_index) {
        auto directory = slot_directory(page);
        if (slot_index >= directory.size() || new_slot.index >= directory.size()) {
            return std::make_error_code(std::errc::io_error);
        }
        std::swap(directory[new_slot.index], directory[slot_index]);
        const auto& adjusted = directory[slot_index];
        new_slot.index = slot_index;
        new_slot.offset = adjusted.offset;
        new_slot.length = adjusted.length;
    }

    out_result.slot = new_slot;
    out_result.wal = wal_result;
    out_result.old_length = old_length;
    return {};
}

std::error_code PageManager::flush_wal() const
{
    if (auto ec = wal_writer_->flush(); ec) {
        return ec;
    }
    return {};
}

std::error_code PageManager::close_wal() const
{
    if (auto ec = wal_writer_->close(); ec) {
        return ec;
    }
    return {};
}

std::shared_ptr<WalWriter> PageManager::wal_writer() const noexcept
{
    return wal_writer_;
}

}  // namespace bored::storage
