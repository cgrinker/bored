#pragma once

#include "bored/storage/page_format.hpp"
#include "bored/storage/wal_format.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

namespace bored::storage {

constexpr std::uint32_t kCrc32cInit = 0xFFFFFFFFu;

std::uint32_t crc32c_extend(std::uint32_t state, std::span<const std::byte> data);
constexpr std::uint32_t crc32c_finalize(std::uint32_t state)
{
    return state ^ 0xFFFFFFFFu;
}
inline std::uint32_t crc32c(std::span<const std::byte> data)
{
    return crc32c_finalize(crc32c_extend(kCrc32cInit, data));
}

std::uint32_t compute_page_checksum(std::span<const std::byte> page);
void update_page_checksum(std::span<std::byte> page);
bool verify_page_checksum(std::span<const std::byte> page);

std::uint32_t compute_wal_checksum(const WalRecordHeader& header, std::span<const std::byte> payload);
void apply_wal_checksum(WalRecordHeader& header, std::span<const std::byte> payload);
bool verify_wal_checksum(const WalRecordHeader& header, std::span<const std::byte> payload);

}  // namespace bored::storage
