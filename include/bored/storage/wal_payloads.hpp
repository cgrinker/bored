#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

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

bool encode_wal_tuple_insert(std::span<std::byte> buffer,
                             const WalTupleMeta& meta,
                             std::span<const std::byte> tuple_data);

bool encode_wal_tuple_delete(std::span<std::byte> buffer, const WalTupleMeta& meta);

bool encode_wal_tuple_update(std::span<std::byte> buffer,
                             const WalTupleUpdateMeta& meta,
                             std::span<const std::byte> new_tuple_data);

std::optional<WalTupleMeta> decode_wal_tuple_meta(std::span<const std::byte> buffer);
std::optional<WalTupleUpdateMeta> decode_wal_tuple_update_meta(std::span<const std::byte> buffer);

std::span<const std::byte> wal_tuple_payload(std::span<const std::byte> buffer, const WalTupleMeta& meta);
std::span<const std::byte> wal_tuple_update_payload(std::span<const std::byte> buffer, const WalTupleUpdateMeta& meta);

}  // namespace bored::storage
