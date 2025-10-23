#include "bored/storage/page_manager.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <vector>

#include "bored/storage/free_space_map_persistence.hpp"

namespace bored::storage {

namespace {

std::span<const std::byte> as_const_span(std::span<std::byte> buffer)
{
    return {buffer.data(), buffer.size()};
}

constexpr std::size_t kOverflowChunkCapacity = kPageSize - sizeof(PageHeader) - sizeof(WalOverflowChunkMeta);
static_assert(kOverflowChunkCapacity > 0U, "Overflow chunk capacity must be positive");

}  // namespace

PageManager::PageManager(FreeSpaceMap* fsm, std::shared_ptr<WalWriter> wal_writer, Config config)
    : fsm_{fsm}
    , wal_writer_{std::move(wal_writer)}
    , config_{config}
    , next_overflow_page_id_{config_.overflow_page_start}
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

std::uint32_t PageManager::allocate_overflow_page_id() const
{
    if (next_overflow_page_id_ == 0U) {
        next_overflow_page_id_ = config_.overflow_page_start;
    }
    auto id = next_overflow_page_id_;
    ++next_overflow_page_id_;
    return id;
}

std::error_code PageManager::build_overflow_truncate_payload(const WalTupleMeta& owner_meta,
                                                             const OverflowTupleHeader& header,
                                                             std::vector<WalOverflowChunkMeta>& chunk_metas,
                                                             std::vector<std::vector<std::byte>>& chunk_payloads,
                                                             WalOverflowTruncateMeta& truncate_meta) const
{
    chunk_metas.clear();
    chunk_payloads.clear();

    truncate_meta.owner = owner_meta;
    truncate_meta.first_overflow_page_id = header.first_overflow_page_id;
    truncate_meta.released_page_count = header.chunk_count;
    truncate_meta.reserved = 0U;

    if (header.chunk_count == 0U) {
        return {};
    }

    chunk_metas.reserve(header.chunk_count);
    chunk_payloads.reserve(header.chunk_count);

    std::uint32_t current_page = header.first_overflow_page_id;

    for (std::uint16_t index = 0; index < header.chunk_count; ++index) {
        auto cache_it = overflow_cache_.find(current_page);
        if (cache_it == overflow_cache_.end()) {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }

        const auto& entry = cache_it->second;
        if (entry.meta.overflow_page_id != current_page) {
            return std::make_error_code(std::errc::io_error);
        }

        chunk_metas.push_back(entry.meta);
        chunk_payloads.push_back(entry.payload);
        current_page = entry.meta.next_overflow_page_id;
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

    if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
        return std::make_error_code(std::errc::value_too_large);
    }

    const auto page_const = as_const_span(page);

    if (auto plan = prepare_append_tuple(page_const, payload.size())) {
        const auto& header = page_header(page_const);

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
        out_result.used_overflow = false;
        out_result.logical_length = payload.size();
        out_result.inline_length = payload.size();
        out_result.overflow_page_ids.clear();
        return {};
    }

    const auto& header = page_header(page_const);

    const auto max_stub_inline = static_cast<std::size_t>(
        std::numeric_limits<std::uint16_t>::max() - overflow_tuple_header_size());

    std::size_t inline_target = payload.size() > 0U ? payload.size() - 1U : 0U;
    inline_target = std::min(inline_target, config_.overflow_inline_prefix);
    inline_target = std::min(inline_target, max_stub_inline);

    std::optional<AppendTuplePlan> stub_plan;
    std::size_t inline_length = inline_target;

    for (std::size_t attempt = 0; attempt < 2 && !stub_plan; ++attempt) {
        if (inline_length >= payload.size() && payload.size() > 0U) {
            inline_length = payload.size() - 1U;
        }

        if (inline_length > max_stub_inline) {
            inline_length = max_stub_inline;
        }

        const auto stub_size = overflow_tuple_header_size() + inline_length;
        if (stub_size <= std::numeric_limits<std::uint16_t>::max()) {
            stub_plan = prepare_append_tuple(page_const, stub_size);
        }

        if (!stub_plan) {
            inline_length = 0U;
        }
    }

    if (!stub_plan) {
        return std::make_error_code(std::errc::no_buffer_space);
    }

    if (inline_length >= payload.size()) {
        inline_length = payload.size() > 0U ? payload.size() - 1U : 0U;
    }

    std::size_t overflow_bytes = payload.size() - inline_length;
    if (overflow_bytes == 0U) {
        return std::make_error_code(std::errc::no_buffer_space);
    }

    const auto chunk_capacity = kOverflowChunkCapacity;
    std::size_t chunk_count = (overflow_bytes + chunk_capacity - 1U) / chunk_capacity;
    if (chunk_count == 0U || chunk_count > std::numeric_limits<std::uint16_t>::max()) {
        return std::make_error_code(std::errc::value_too_large);
    }

    std::vector<std::uint32_t> overflow_page_ids;
    overflow_page_ids.reserve(chunk_count);
    for (std::size_t index = 0; index < chunk_count; ++index) {
        overflow_page_ids.push_back(allocate_overflow_page_id());
        if (overflow_page_ids.back() == 0U) {
            return std::make_error_code(std::errc::io_error);
        }
    }

    OverflowTupleHeader stub_header{};
    stub_header.logical_length = static_cast<std::uint32_t>(payload.size());
    stub_header.first_overflow_page_id = overflow_page_ids.front();
    stub_header.inline_length = static_cast<std::uint32_t>(inline_length);
    stub_header.chunk_count = static_cast<std::uint16_t>(chunk_count);

    const auto stub_size = overflow_tuple_header_size() + inline_length;
    std::vector<std::byte> stub_bytes(stub_size);
    std::memcpy(stub_bytes.data(), &stub_header, sizeof(stub_header));
    if (inline_length > 0U) {
        std::memcpy(stub_bytes.data() + overflow_tuple_header_size(), payload.data(), inline_length);
    }

    WalTupleMeta meta{};
    meta.page_id = header.page_id;
    meta.slot_index = stub_plan->slot_index;
    meta.tuple_length = static_cast<std::uint16_t>(stub_bytes.size());
    meta.row_id = row_id;

    std::vector<std::byte> wal_buffer(wal_tuple_insert_payload_size(meta.tuple_length));
    auto wal_buffer_span = std::span<std::byte>(wal_buffer.data(), wal_buffer.size());
    if (!encode_wal_tuple_insert(wal_buffer_span,
                                 meta,
                                 std::span<const std::byte>(stub_bytes.data(), stub_bytes.size()))) {
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

    std::size_t chunk_offset = inline_length;
    for (std::size_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index) {
        const auto chunk_length = std::min(chunk_capacity, payload.size() - chunk_offset);

        if (chunk_offset > std::numeric_limits<std::uint16_t>::max()) {
            return std::make_error_code(std::errc::value_too_large);
        }
        if (chunk_length > std::numeric_limits<std::uint16_t>::max()) {
            return std::make_error_code(std::errc::value_too_large);
        }

        WalOverflowChunkMeta chunk_meta{};
        chunk_meta.owner = meta;
        chunk_meta.overflow_page_id = overflow_page_ids[chunk_index];
        chunk_meta.next_overflow_page_id = (chunk_index + 1U < chunk_count) ? overflow_page_ids[chunk_index + 1U] : 0U;
        chunk_meta.chunk_offset = static_cast<std::uint16_t>(chunk_offset);
        chunk_meta.chunk_length = static_cast<std::uint16_t>(chunk_length);
        chunk_meta.chunk_index = static_cast<std::uint16_t>(chunk_index);
        chunk_meta.flags = 0U;
        if (chunk_index == 0U) {
            chunk_meta.flags |= static_cast<std::uint16_t>(WalOverflowChunkFlag::ChainStart);
        }
        if (chunk_index + 1U == chunk_count) {
            chunk_meta.flags |= static_cast<std::uint16_t>(WalOverflowChunkFlag::ChainEnd);
        }

        std::vector<std::byte> chunk_buffer(wal_overflow_chunk_payload_size(chunk_meta.chunk_length));
        auto chunk_span = std::span<std::byte>(chunk_buffer.data(), chunk_buffer.size());
        auto chunk_payload = payload.subspan(chunk_offset, chunk_length);
        if (!encode_wal_overflow_chunk(chunk_span, chunk_meta, chunk_payload)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        overflow_cache_[chunk_meta.overflow_page_id] = OverflowChunkCacheEntry{chunk_meta,
                                                                               std::vector<std::byte>(chunk_payload.begin(), chunk_payload.end())};

        WalRecordDescriptor chunk_descriptor{};
        chunk_descriptor.type = WalRecordType::TupleOverflowChunk;
        chunk_descriptor.page_id = chunk_meta.overflow_page_id;
        chunk_descriptor.flags = WalRecordFlag::None;
        chunk_descriptor.payload = std::span<const std::byte>(chunk_span.data(), chunk_span.size());

        WalAppendResult chunk_result{};
        if (auto chunk_ec = wal_writer_->append_record(chunk_descriptor, chunk_result); chunk_ec) {
            return chunk_ec;
        }

        chunk_offset += chunk_length;
    }

    auto stub_span = std::span<const std::byte>(stub_bytes.data(), stub_bytes.size());
    auto slot = append_tuple(page, stub_span, wal_result.lsn, fsm_);
    if (!slot) {
        return std::make_error_code(std::errc::io_error);
    }

    auto& page_header_ref = page_header(page);
    page_header_ref.flags |= static_cast<std::uint16_t>(PageFlag::HasOverflow);

    out_result.slot = *slot;
    out_result.wal = wal_result;
    out_result.used_overflow = true;
    out_result.logical_length = payload.size();
    out_result.inline_length = inline_length;
    out_result.overflow_page_ids = overflow_page_ids;
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

    WalTupleMeta before_meta{};
    before_meta.page_id = header.page_id;
    before_meta.slot_index = slot_index;
    before_meta.tuple_length = static_cast<std::uint16_t>(tuple_view.size());
    before_meta.row_id = row_id;

    std::vector<std::byte> before_buffer(wal_tuple_insert_payload_size(before_meta.tuple_length));
    auto before_span = std::span<std::byte>(before_buffer.data(), before_buffer.size());
    if (!encode_wal_tuple_insert(before_span, before_meta, tuple_view)) {
        return std::make_error_code(std::errc::io_error);
    }

    WalRecordDescriptor before_descriptor{};
    before_descriptor.type = WalRecordType::TupleBeforeImage;
    before_descriptor.page_id = header.page_id;
    before_descriptor.flags = WalRecordFlag::None;
    before_descriptor.payload = std::span<const std::byte>(before_span.data(), before_span.size());

    WalAppendResult before_result{};
    if (auto before_ec = wal_writer_->append_record(before_descriptor, before_result); before_ec) {
        return before_ec;
    }

    std::optional<OverflowTupleHeader> overflow_header;
    std::vector<WalOverflowChunkMeta> truncate_chunk_metas;
    std::vector<std::vector<std::byte>> truncate_chunk_payloads;
    WalOverflowTruncateMeta truncate_meta{};

    if (auto header_opt = parse_overflow_tuple(tuple_view)) {
        if (header_opt->chunk_count > 0U) {
            overflow_header = header_opt;
            if (auto ec = build_overflow_truncate_payload(before_meta,
                                                          *header_opt,
                                                          truncate_chunk_metas,
                                                          truncate_chunk_payloads,
                                                          truncate_meta);
                ec) {
                return ec;
            }

            std::vector<std::span<const std::byte>> payload_views;
            payload_views.reserve(truncate_chunk_payloads.size());
            for (const auto& payload : truncate_chunk_payloads) {
                payload_views.emplace_back(payload.data(), payload.size());
            }

            auto chunk_meta_span = std::span<const WalOverflowChunkMeta>(truncate_chunk_metas.data(), truncate_chunk_metas.size());
            auto payload_span = std::span<const std::span<const std::byte>>(payload_views.data(), payload_views.size());
            std::vector<std::byte> truncate_buffer(wal_overflow_truncate_payload_size(chunk_meta_span));
            auto truncate_span = std::span<std::byte>(truncate_buffer.data(), truncate_buffer.size());
            if (!encode_wal_overflow_truncate(truncate_span, truncate_meta, chunk_meta_span, payload_span)) {
                return std::make_error_code(std::errc::invalid_argument);
            }

            WalRecordDescriptor truncate_descriptor{};
            truncate_descriptor.type = WalRecordType::TupleOverflowTruncate;
            truncate_descriptor.page_id = truncate_meta.first_overflow_page_id;
            truncate_descriptor.flags = WalRecordFlag::None;
            truncate_descriptor.payload = std::span<const std::byte>(truncate_span.data(), truncate_span.size());

            WalAppendResult truncate_result{};
            if (auto truncate_ec = wal_writer_->append_record(truncate_descriptor, truncate_result); truncate_ec) {
                return truncate_ec;
            }

            for (const auto& meta_entry : truncate_chunk_metas) {
                overflow_cache_.erase(meta_entry.overflow_page_id);
            }
        }
    }

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

    WalTupleMeta before_meta{};
    before_meta.page_id = header.page_id;
    before_meta.slot_index = slot_index;
    before_meta.tuple_length = old_length;
    before_meta.row_id = row_id;

    std::vector<std::byte> before_buffer(wal_tuple_insert_payload_size(before_meta.tuple_length));
    auto before_span = std::span<std::byte>(before_buffer.data(), before_buffer.size());
    if (!encode_wal_tuple_insert(before_span, before_meta, current_tuple)) {
        return std::make_error_code(std::errc::io_error);
    }

    WalRecordDescriptor before_descriptor{};
    before_descriptor.type = WalRecordType::TupleBeforeImage;
    before_descriptor.page_id = header.page_id;
    before_descriptor.flags = WalRecordFlag::None;
    before_descriptor.payload = std::span<const std::byte>(before_span.data(), before_span.size());

    WalAppendResult before_result{};
    if (auto before_ec = wal_writer_->append_record(before_descriptor, before_result); before_ec) {
        return before_ec;
    }

    std::vector<WalOverflowChunkMeta> truncate_chunk_metas;
    std::vector<std::vector<std::byte>> truncate_chunk_payloads;
    WalOverflowTruncateMeta truncate_meta{};

    if (auto overflow_header = parse_overflow_tuple(current_tuple)) {
        if (overflow_header->chunk_count > 0U) {
            if (auto ec = build_overflow_truncate_payload(before_meta,
                                                          *overflow_header,
                                                          truncate_chunk_metas,
                                                          truncate_chunk_payloads,
                                                          truncate_meta);
                ec) {
                return ec;
            }

            std::vector<std::span<const std::byte>> payload_views;
            payload_views.reserve(truncate_chunk_payloads.size());
            for (const auto& payload : truncate_chunk_payloads) {
                payload_views.emplace_back(payload.data(), payload.size());
            }

            auto chunk_meta_span = std::span<const WalOverflowChunkMeta>(truncate_chunk_metas.data(), truncate_chunk_metas.size());
            auto payload_span = std::span<const std::span<const std::byte>>(payload_views.data(), payload_views.size());
            std::vector<std::byte> truncate_buffer(wal_overflow_truncate_payload_size(chunk_meta_span));
            auto truncate_span = std::span<std::byte>(truncate_buffer.data(), truncate_buffer.size());
            if (!encode_wal_overflow_truncate(truncate_span, truncate_meta, chunk_meta_span, payload_span)) {
                return std::make_error_code(std::errc::invalid_argument);
            }

            WalRecordDescriptor truncate_descriptor{};
            truncate_descriptor.type = WalRecordType::TupleOverflowTruncate;
            truncate_descriptor.page_id = truncate_meta.first_overflow_page_id;
            truncate_descriptor.flags = WalRecordFlag::None;
            truncate_descriptor.payload = std::span<const std::byte>(truncate_span.data(), truncate_span.size());

            WalAppendResult truncate_result{};
            if (auto truncate_ec = wal_writer_->append_record(truncate_descriptor, truncate_result); truncate_ec) {
                return truncate_ec;
            }

            for (const auto& meta_entry : truncate_chunk_metas) {
                overflow_cache_.erase(meta_entry.overflow_page_id);
            }
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

std::error_code PageManager::persist_free_space_map(const std::filesystem::path& path) const
{
    if (!fsm_) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    return FreeSpaceMapPersistence::write_snapshot(*fsm_, path);
}

std::error_code PageManager::load_free_space_map(const std::filesystem::path& path) const
{
    if (!fsm_) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto ec = FreeSpaceMapPersistence::load_snapshot(path, *fsm_);
    if (ec == std::make_error_code(std::errc::no_such_file_or_directory)) {
        return {};
    }
    return ec;
}

std::shared_ptr<WalWriter> PageManager::wal_writer() const noexcept
{
    return wal_writer_;
}

}  // namespace bored::storage
