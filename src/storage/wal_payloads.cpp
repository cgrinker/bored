#include "bored/storage/wal_payloads.hpp"

#include <cstring>
#include <limits>
#include <vector>

namespace bored::storage {

namespace {

bool fits(std::span<const std::byte> buffer, std::size_t required)
{
    return buffer.size() >= required;
}

bool fits(std::span<std::byte> buffer, std::size_t required)
{
    return buffer.size() >= required;
}

}  // namespace

bool encode_wal_tuple_insert(std::span<std::byte> buffer,
                             const WalTupleMeta& meta,
                             std::span<const std::byte> tuple_data)
{
    if (meta.tuple_length != tuple_data.size()) {
        return false;
    }

    const auto required = wal_tuple_insert_payload_size(meta.tuple_length);
    if (!fits(buffer, required)) {
        return false;
    }

    std::memcpy(buffer.data(), &meta, sizeof(WalTupleMeta));
    std::memcpy(buffer.data() + sizeof(WalTupleMeta), tuple_data.data(), tuple_data.size());
    return true;
}

bool encode_wal_tuple_delete(std::span<std::byte> buffer, const WalTupleMeta& meta)
{
    if (meta.tuple_length != 0U) {
        return false;
    }

    if (!fits(buffer, wal_tuple_delete_payload_size())) {
        return false;
    }

    std::memcpy(buffer.data(), &meta, sizeof(WalTupleMeta));
    return true;
}

bool encode_wal_tuple_update(std::span<std::byte> buffer,
                             const WalTupleUpdateMeta& meta,
                             std::span<const std::byte> new_tuple_data)
{
    if (meta.base.tuple_length != new_tuple_data.size()) {
        return false;
    }

    const auto required = wal_tuple_update_payload_size(meta.base.tuple_length);
    if (!fits(buffer, required)) {
        return false;
    }

    std::memcpy(buffer.data(), &meta, sizeof(WalTupleUpdateMeta));
    std::memcpy(buffer.data() + sizeof(WalTupleUpdateMeta), new_tuple_data.data(), new_tuple_data.size());
    return true;
}

std::optional<WalTupleMeta> decode_wal_tuple_meta(std::span<const std::byte> buffer)
{
    if (!fits(buffer, sizeof(WalTupleMeta))) {
        return std::nullopt;
    }

    WalTupleMeta meta{};
    std::memcpy(&meta, buffer.data(), sizeof(WalTupleMeta));
    return meta;
}

std::optional<WalTupleUpdateMeta> decode_wal_tuple_update_meta(std::span<const std::byte> buffer)
{
    if (!fits(buffer, sizeof(WalTupleUpdateMeta))) {
        return std::nullopt;
    }

    WalTupleUpdateMeta meta{};
    std::memcpy(&meta, buffer.data(), sizeof(WalTupleUpdateMeta));
    return meta;
}

std::span<const std::byte> wal_tuple_payload(std::span<const std::byte> buffer, const WalTupleMeta& meta)
{
    const auto header_size = sizeof(WalTupleMeta);
    if (meta.tuple_length == 0U) {
        return {};
    }

    if (!fits(buffer, header_size + meta.tuple_length)) {
        return {};
    }

    return buffer.subspan(header_size, meta.tuple_length);
}

std::span<const std::byte> wal_tuple_update_payload(std::span<const std::byte> buffer, const WalTupleUpdateMeta& meta)
{
    const auto header_size = sizeof(WalTupleUpdateMeta);
    if (meta.base.tuple_length == 0U) {
        return {};
    }

    if (!fits(buffer, header_size + meta.base.tuple_length)) {
        return {};
    }

    return buffer.subspan(header_size, meta.base.tuple_length);
}

std::size_t wal_tuple_before_image_payload_size(std::uint16_t tuple_length,
                                                std::span<const WalOverflowChunkMeta> chunk_metas)
{
    std::size_t total = sizeof(WalTupleBeforeImageHeader) + tuple_length;
    for (const auto& chunk_meta : chunk_metas) {
        total += sizeof(WalOverflowChunkMeta) + chunk_meta.chunk_length;
    }
    return total;
}

bool encode_wal_tuple_before_image(std::span<std::byte> buffer,
                                   const WalTupleMeta& meta,
                                   std::span<const std::byte> tuple_data,
                                   std::span<const WalOverflowChunkMeta> chunk_metas,
                                   std::span<const std::span<const std::byte>> chunk_payloads,
                                   std::uint64_t previous_page_lsn,
                                   std::uint16_t previous_free_start,
                                   std::uint16_t previous_tuple_offset)
{
    if (meta.tuple_length != tuple_data.size()) {
        return false;
    }

    if (chunk_metas.size() != chunk_payloads.size()) {
        return false;
    }

    const auto required = wal_tuple_before_image_payload_size(meta.tuple_length, chunk_metas);
    if (!fits(buffer, required)) {
        return false;
    }

    WalTupleBeforeImageHeader header{};
    header.meta = meta;
    header.overflow_chunk_count = static_cast<std::uint32_t>(chunk_metas.size());
    header.previous_page_lsn = previous_page_lsn;
    header.previous_free_start = previous_free_start;
    header.previous_tuple_offset = previous_tuple_offset;

    std::memcpy(buffer.data(), &header, sizeof(WalTupleBeforeImageHeader));

    std::size_t offset = sizeof(WalTupleBeforeImageHeader);
    if (!tuple_data.empty()) {
        std::memcpy(buffer.data() + offset, tuple_data.data(), tuple_data.size());
    }
    offset += tuple_data.size();

    for (std::size_t index = 0; index < chunk_metas.size(); ++index) {
        const auto& chunk_meta = chunk_metas[index];
        const auto& payload = chunk_payloads[index];
        if (payload.size() != chunk_meta.chunk_length) {
            return false;
        }

        std::memcpy(buffer.data() + offset, &chunk_meta, sizeof(WalOverflowChunkMeta));
        offset += sizeof(WalOverflowChunkMeta);

        if (!payload.empty()) {
            std::memcpy(buffer.data() + offset, payload.data(), payload.size());
        }
        offset += payload.size();
    }

    return true;
}

bool encode_wal_overflow_chunk(std::span<std::byte> buffer,
                               const WalOverflowChunkMeta& meta,
                               std::span<const std::byte> chunk_data)
{
    if (meta.chunk_length != chunk_data.size()) {
        return false;
    }

    if (meta.chunk_length == 0U) {
        return false;
    }

    const auto required = wal_overflow_chunk_payload_size(meta.chunk_length);
    if (!fits(buffer, required)) {
        return false;
    }

    std::memcpy(buffer.data(), &meta, sizeof(WalOverflowChunkMeta));
    std::memcpy(buffer.data() + sizeof(WalOverflowChunkMeta), chunk_data.data(), chunk_data.size());
    return true;
}

std::size_t wal_overflow_truncate_payload_size(std::span<const WalOverflowChunkMeta> chunk_metas)
{
    std::size_t total = sizeof(WalOverflowTruncateMeta);
    for (const auto& chunk_meta : chunk_metas) {
        total += sizeof(WalOverflowChunkMeta) + chunk_meta.chunk_length;
    }
    return total;
}

std::size_t wal_checkpoint_payload_size(std::size_t dirty_page_count, std::size_t active_transaction_count)
{
    return sizeof(WalCheckpointHeader)
        + (dirty_page_count * sizeof(WalCheckpointDirtyPageEntry))
        + (active_transaction_count * sizeof(WalCheckpointTxnEntry));
}

std::size_t wal_compaction_payload_size(std::size_t entry_count)
{
    return sizeof(WalCompactionHeader) + (entry_count * sizeof(WalCompactionEntry));
}

bool encode_wal_overflow_truncate(std::span<std::byte> buffer,
                                  const WalOverflowTruncateMeta& meta,
                                  std::span<const WalOverflowChunkMeta> chunk_metas,
                                  std::span<const std::span<const std::byte>> chunk_payloads)
{
    if (chunk_metas.size() != chunk_payloads.size()) {
        return false;
    }

    if (meta.released_page_count != chunk_metas.size()) {
        return false;
    }

    const auto required = wal_overflow_truncate_payload_size(chunk_metas);
    if (!fits(buffer, required)) {
        return false;
    }

    std::memcpy(buffer.data(), &meta, sizeof(WalOverflowTruncateMeta));

    std::size_t offset = sizeof(WalOverflowTruncateMeta);
    for (std::size_t index = 0; index < chunk_metas.size(); ++index) {
        const auto& chunk_meta = chunk_metas[index];
        const auto& payload = chunk_payloads[index];
        if (payload.size() != chunk_meta.chunk_length) {
            return false;
        }

        std::memcpy(buffer.data() + offset, &chunk_meta, sizeof(WalOverflowChunkMeta));
        offset += sizeof(WalOverflowChunkMeta);

        if (!payload.empty()) {
            std::memcpy(buffer.data() + offset, payload.data(), payload.size());
        }
        offset += payload.size();
    }
    return true;
}

bool encode_wal_commit(std::span<std::byte> buffer, const WalCommitHeader& header)
{
    if (!fits(buffer, sizeof(WalCommitHeader))) {
        return false;
    }

    std::memcpy(buffer.data(), &header, sizeof(WalCommitHeader));
    return true;
}

bool encode_wal_checkpoint(std::span<std::byte> buffer,
                           const WalCheckpointHeader& header,
                           std::span<const WalCheckpointDirtyPageEntry> dirty_pages,
                           std::span<const WalCheckpointTxnEntry> active_transactions)
{
    if (dirty_pages.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    if (active_transactions.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }

    if (header.dirty_page_count != dirty_pages.size()) {
        return false;
    }
    if (header.active_transaction_count != active_transactions.size()) {
        return false;
    }

    const auto required = wal_checkpoint_payload_size(dirty_pages.size(), active_transactions.size());
    if (!fits(buffer, required)) {
        return false;
    }

    std::memcpy(buffer.data(), &header, sizeof(WalCheckpointHeader));

    std::size_t offset = sizeof(WalCheckpointHeader);
    const auto dirty_bytes = dirty_pages.size() * sizeof(WalCheckpointDirtyPageEntry);
    if (dirty_bytes != 0U) {
        std::memcpy(buffer.data() + offset, dirty_pages.data(), dirty_bytes);
        offset += dirty_bytes;
    }

    const auto txn_bytes = active_transactions.size() * sizeof(WalCheckpointTxnEntry);
    if (txn_bytes != 0U) {
        std::memcpy(buffer.data() + offset, active_transactions.data(), txn_bytes);
        offset += txn_bytes;
    }

    (void)offset;
    return true;
}

bool encode_wal_compaction(std::span<std::byte> buffer,
                           const WalCompactionHeader& header,
                           std::span<const WalCompactionEntry> entries)
{
    WalCompactionHeader local_header = header;
    if (entries.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }

    local_header.entry_count = static_cast<std::uint32_t>(entries.size());

    const auto required = wal_compaction_payload_size(entries.size());
    if (!fits(buffer, required)) {
        return false;
    }

    std::memcpy(buffer.data(), &local_header, sizeof(WalCompactionHeader));
    if (!entries.empty()) {
        std::memcpy(buffer.data() + sizeof(WalCompactionHeader), entries.data(), entries.size() * sizeof(WalCompactionEntry));
    }
    return true;
}

std::optional<WalOverflowChunkMeta> decode_wal_overflow_chunk_meta(std::span<const std::byte> buffer)
{
    if (!fits(buffer, sizeof(WalOverflowChunkMeta))) {
        return std::nullopt;
    }

    WalOverflowChunkMeta meta{};
    std::memcpy(&meta, buffer.data(), sizeof(WalOverflowChunkMeta));

    if (!fits(buffer, sizeof(WalOverflowChunkMeta) + meta.chunk_length)) {
        return std::nullopt;
    }

    if (meta.chunk_length == 0U) {
        return std::nullopt;
    }

    return meta;
}

std::optional<WalOverflowTruncateMeta> decode_wal_overflow_truncate_meta(std::span<const std::byte> buffer)
{
    if (!fits(buffer, sizeof(WalOverflowTruncateMeta))) {
        return std::nullopt;
    }

    WalOverflowTruncateMeta meta{};
    std::memcpy(&meta, buffer.data(), sizeof(WalOverflowTruncateMeta));
    return meta;
}

std::optional<WalCommitHeader> decode_wal_commit(std::span<const std::byte> buffer)
{
    if (!fits(buffer, sizeof(WalCommitHeader))) {
        return std::nullopt;
    }

    WalCommitHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(WalCommitHeader));
    return header;
}

std::span<const std::byte> wal_overflow_chunk_payload(std::span<const std::byte> buffer,
                                                      const WalOverflowChunkMeta& meta)
{
    const auto header_size = sizeof(WalOverflowChunkMeta);
    if (!fits(buffer, header_size + meta.chunk_length)) {
        return {};
    }

    return buffer.subspan(header_size, meta.chunk_length);
}

std::optional<std::vector<WalOverflowTruncateChunkView>> decode_wal_overflow_truncate_chunks(std::span<const std::byte> buffer,
                                                                                             const WalOverflowTruncateMeta& meta)
{
    if (!fits(buffer, sizeof(WalOverflowTruncateMeta))) {
        return std::nullopt;
    }

    std::size_t offset = sizeof(WalOverflowTruncateMeta);
    std::vector<WalOverflowTruncateChunkView> results;
    results.reserve(meta.released_page_count);

    for (std::uint32_t index = 0; index < meta.released_page_count; ++index) {
        if (!fits(buffer.subspan(offset), sizeof(WalOverflowChunkMeta))) {
            return std::nullopt;
        }

        WalOverflowChunkMeta chunk_meta{};
        std::memcpy(&chunk_meta, buffer.data() + offset, sizeof(WalOverflowChunkMeta));
        offset += sizeof(WalOverflowChunkMeta);

        if (!fits(buffer.subspan(offset), chunk_meta.chunk_length)) {
            return std::nullopt;
        }

        auto payload = buffer.subspan(offset, chunk_meta.chunk_length);
        offset += chunk_meta.chunk_length;

        results.push_back(WalOverflowTruncateChunkView{chunk_meta, payload});
    }

    return results;
}

std::optional<WalCompactionView> decode_wal_compaction(std::span<const std::byte> buffer)
{
    if (!fits(buffer, sizeof(WalCompactionHeader))) {
        return std::nullopt;
    }

    WalCompactionHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(WalCompactionHeader));

    const auto entry_bytes = static_cast<std::size_t>(header.entry_count) * sizeof(WalCompactionEntry);
    if (!fits(buffer, sizeof(WalCompactionHeader) + entry_bytes)) {
        return std::nullopt;
    }

    WalCompactionView view{};
    view.header = header;
    view.entries.resize(header.entry_count);
    if (!view.entries.empty()) {
        std::memcpy(view.entries.data(), buffer.data() + sizeof(WalCompactionHeader), entry_bytes);
    }
    return view;
}

std::optional<WalTupleBeforeImageView> decode_wal_tuple_before_image(std::span<const std::byte> buffer)
{
    if (!fits(buffer, sizeof(WalTupleBeforeImageHeader))) {
        return std::nullopt;
    }

    WalTupleBeforeImageHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(WalTupleBeforeImageHeader));

    if (!fits(buffer, sizeof(WalTupleBeforeImageHeader) + header.meta.tuple_length)) {
        return std::nullopt;
    }

    WalTupleBeforeImageView view{};
    view.meta = header.meta;
    view.tuple_payload = buffer.subspan(sizeof(WalTupleBeforeImageHeader), header.meta.tuple_length);
    view.previous_page_lsn = header.previous_page_lsn;
    view.previous_free_start = header.previous_free_start;
    view.previous_tuple_offset = header.previous_tuple_offset;

    std::size_t offset = sizeof(WalTupleBeforeImageHeader) + header.meta.tuple_length;
    view.overflow_chunks.reserve(header.overflow_chunk_count);

    for (std::uint32_t index = 0; index < header.overflow_chunk_count; ++index) {
        if (!fits(buffer.subspan(offset), sizeof(WalOverflowChunkMeta))) {
            return std::nullopt;
        }

        WalOverflowChunkMeta chunk_meta{};
        std::memcpy(&chunk_meta, buffer.data() + offset, sizeof(WalOverflowChunkMeta));
        offset += sizeof(WalOverflowChunkMeta);

        if (!fits(buffer.subspan(offset), chunk_meta.chunk_length)) {
            return std::nullopt;
        }

        auto payload = buffer.subspan(offset, chunk_meta.chunk_length);
        offset += chunk_meta.chunk_length;

        view.overflow_chunks.push_back(WalTupleBeforeImageChunkView{chunk_meta, payload});
    }

    return view;
}

std::optional<WalCheckpointView> decode_wal_checkpoint(std::span<const std::byte> buffer)
{
    if (!fits(buffer, sizeof(WalCheckpointHeader))) {
        return std::nullopt;
    }

    WalCheckpointHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(WalCheckpointHeader));

    const auto required = wal_checkpoint_payload_size(header.dirty_page_count, header.active_transaction_count);
    if (!fits(buffer, required)) {
        return std::nullopt;
    }

    WalCheckpointView view{};
    view.header = header;

    std::size_t offset = sizeof(WalCheckpointHeader);

    if (header.dirty_page_count != 0U) {
        view.dirty_pages.resize(header.dirty_page_count);
        const auto dirty_bytes = view.dirty_pages.size() * sizeof(WalCheckpointDirtyPageEntry);
        std::memcpy(view.dirty_pages.data(), buffer.data() + offset, dirty_bytes);
        offset += dirty_bytes;
    }

    if (header.active_transaction_count != 0U) {
        view.active_transactions.resize(header.active_transaction_count);
        const auto txn_bytes = view.active_transactions.size() * sizeof(WalCheckpointTxnEntry);
        std::memcpy(view.active_transactions.data(), buffer.data() + offset, txn_bytes);
        offset += txn_bytes;
    }

    return view;
}

}  // namespace bored::storage
