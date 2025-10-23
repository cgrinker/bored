#include "bored/storage/wal_payloads.hpp"

#include <cstring>
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
                                   std::span<const std::span<const std::byte>> chunk_payloads)
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

}  // namespace bored::storage
