#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace bored::storage {

struct alignas(8) WalTupleMeta final {
    std::uint32_t page_id = 0U;
    std::uint16_t slot_index = 0U;
    std::uint16_t tuple_length = 0U;
    std::uint64_t row_id = 0U;
};

struct alignas(8) WalTupleUpdateMeta final {
    WalTupleMeta base{};
    std::uint16_t old_length = 0U;
    std::uint16_t reserved = 0U;
};

enum class WalOverflowChunkFlag : std::uint16_t {
    None = 0,
    ChainStart = 1 << 0,
    ChainEnd = 1 << 1
};

constexpr WalOverflowChunkFlag operator|(WalOverflowChunkFlag lhs, WalOverflowChunkFlag rhs)
{
    return static_cast<WalOverflowChunkFlag>(static_cast<std::uint16_t>(lhs) | static_cast<std::uint16_t>(rhs));
}

constexpr WalOverflowChunkFlag operator&(WalOverflowChunkFlag lhs, WalOverflowChunkFlag rhs)
{
    return static_cast<WalOverflowChunkFlag>(static_cast<std::uint16_t>(lhs) & static_cast<std::uint16_t>(rhs));
}

constexpr bool any(WalOverflowChunkFlag value)
{
    return static_cast<std::uint16_t>(value) != 0U;
}

struct alignas(8) WalOverflowChunkMeta final {
    WalTupleMeta owner{};
    std::uint32_t overflow_page_id = 0U;
    std::uint32_t next_overflow_page_id = 0U;
    std::uint16_t chunk_offset = 0U;
    std::uint16_t chunk_length = 0U;
    std::uint16_t chunk_index = 0U;
    std::uint16_t flags = static_cast<std::uint16_t>(WalOverflowChunkFlag::None);
};

struct alignas(8) WalOverflowTruncateMeta final {
    WalTupleMeta owner{};
    std::uint32_t first_overflow_page_id = 0U;
    std::uint32_t released_page_count = 0U;
    std::uint32_t reserved = 0U;
};

struct WalOverflowTruncateChunkView final {
    WalOverflowChunkMeta meta{};
    std::span<const std::byte> payload{};
};

constexpr std::size_t wal_tuple_insert_payload_size(std::uint16_t tuple_length)
{
    return sizeof(WalTupleMeta) + tuple_length;
}

constexpr std::size_t wal_tuple_delete_payload_size()
{
    return sizeof(WalTupleMeta);
}

constexpr std::size_t wal_tuple_update_payload_size(std::uint16_t new_length)
{
    return sizeof(WalTupleUpdateMeta) + new_length;
}

constexpr std::size_t wal_overflow_chunk_payload_size(std::uint16_t chunk_length)
{
    return sizeof(WalOverflowChunkMeta) + chunk_length;
}

std::size_t wal_overflow_truncate_payload_size(std::span<const WalOverflowChunkMeta> chunk_metas);

bool encode_wal_tuple_insert(std::span<std::byte> buffer,
                             const WalTupleMeta& meta,
                             std::span<const std::byte> tuple_data);

bool encode_wal_tuple_delete(std::span<std::byte> buffer, const WalTupleMeta& meta);

bool encode_wal_tuple_update(std::span<std::byte> buffer,
                             const WalTupleUpdateMeta& meta,
                             std::span<const std::byte> new_tuple_data);

std::optional<WalTupleMeta> decode_wal_tuple_meta(std::span<const std::byte> buffer);
std::optional<WalTupleUpdateMeta> decode_wal_tuple_update_meta(std::span<const std::byte> buffer);

std::optional<WalOverflowChunkMeta> decode_wal_overflow_chunk_meta(std::span<const std::byte> buffer);
std::optional<WalOverflowTruncateMeta> decode_wal_overflow_truncate_meta(std::span<const std::byte> buffer);

std::span<const std::byte> wal_tuple_payload(std::span<const std::byte> buffer, const WalTupleMeta& meta);
std::span<const std::byte> wal_tuple_update_payload(std::span<const std::byte> buffer, const WalTupleUpdateMeta& meta);

bool encode_wal_overflow_chunk(std::span<std::byte> buffer,
                               const WalOverflowChunkMeta& meta,
                               std::span<const std::byte> chunk_data);

bool encode_wal_overflow_truncate(std::span<std::byte> buffer,
                                  const WalOverflowTruncateMeta& meta,
                                  std::span<const WalOverflowChunkMeta> chunk_metas,
                                  std::span<const std::span<const std::byte>> chunk_payloads);

std::span<const std::byte> wal_overflow_chunk_payload(std::span<const std::byte> buffer,
                                                      const WalOverflowChunkMeta& meta);

std::optional<std::vector<WalOverflowTruncateChunkView>> decode_wal_overflow_truncate_chunks(std::span<const std::byte> buffer,
                                                                                             const WalOverflowTruncateMeta& meta);

static_assert(sizeof(WalOverflowChunkMeta) == 32, "WalOverflowChunkMeta expected to be 32 bytes");
static_assert(alignof(WalOverflowChunkMeta) == 8, "WalOverflowChunkMeta requires 8-byte alignment");
static_assert(sizeof(WalOverflowTruncateMeta) == 32, "WalOverflowTruncateMeta expected to be 32 bytes");
static_assert(alignof(WalOverflowTruncateMeta) == 8, "WalOverflowTruncateMeta requires 8-byte alignment");

}  // namespace bored::storage
