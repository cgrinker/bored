#include "bored/storage/checksum.hpp"

#include <array>
#include <cstddef>
#include <cstring>

#if defined(_MSC_VER) || defined(__SSE4_2__)
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <nmmintrin.h>
#define BORED_STORAGE_HAS_SSE42 1
#endif
#endif

namespace bored::storage {

namespace {

constexpr std::array<std::uint32_t, 256> make_crc32c_table()
{
    std::array<std::uint32_t, 256> table{};
    constexpr std::uint32_t poly = 0x1EDC6F41u;
    for (std::uint32_t i = 0; i < table.size(); ++i) {
        std::uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            const bool lsb = (crc & 1u) != 0u;
            crc >>= 1;
            if (lsb) {
                crc ^= poly;
            }
        }
        table[i] = crc;
    }
    return table;
}

constexpr auto kCrc32cTable = make_crc32c_table();

std::uint32_t crc32c_sw(std::uint32_t crc, std::span<const std::byte> data)
{
    for (auto byte : data) {
        const auto value = std::to_integer<std::uint8_t>(byte);
        const auto index = static_cast<std::uint8_t>((crc ^ value) & 0xFFu);
        crc = (crc >> 8U) ^ kCrc32cTable[index];
    }
    return crc;
}

#if defined(BORED_STORAGE_HAS_SSE42)
bool has_sse42()
{
#if defined(_MSC_VER)
    int cpuInfo[4]{};
    __cpuid(cpuInfo, 1);
    return (cpuInfo[2] & (1 << 20)) != 0;
#else
    return __builtin_cpu_supports("sse4.2");
#endif
}

std::uint32_t crc32c_hw(std::uint32_t crc, std::span<const std::byte> data)
{
    const auto* bytes = reinterpret_cast<const unsigned char*>(data.data());
    std::size_t remaining = data.size();

#if defined(__x86_64__) || defined(_M_X64)
    while (remaining >= sizeof(std::uint64_t)) {
        std::uint64_t chunk;
        std::memcpy(&chunk, bytes, sizeof(chunk));
        crc = static_cast<std::uint32_t>(_mm_crc32_u64(crc, chunk));
        bytes += sizeof(std::uint64_t);
        remaining -= sizeof(std::uint64_t);
    }
#endif
    while (remaining >= sizeof(std::uint32_t)) {
        std::uint32_t chunk;
        std::memcpy(&chunk, bytes, sizeof(chunk));
        crc = _mm_crc32_u32(crc, chunk);
        bytes += sizeof(std::uint32_t);
        remaining -= sizeof(std::uint32_t);
    }
    while (remaining > 0) {
        crc = _mm_crc32_u8(crc, *bytes++);
        --remaining;
    }

    return crc;
}
#endif

std::uint32_t dispatch_crc32c(std::uint32_t crc, std::span<const std::byte> data)
{
#if defined(BORED_STORAGE_HAS_SSE42)
    if (has_sse42()) {
        return crc32c_hw(crc, data);
    }
#endif
    return crc32c_sw(crc, data);
}

}  // namespace

std::uint32_t crc32c_extend(std::uint32_t state, std::span<const std::byte> data)
{
    return dispatch_crc32c(state, data);
}

std::uint32_t compute_page_checksum(std::span<const std::byte> page)
{
    if (page.size() != kPageSize) {
        return 0U;
    }
    auto buffer = std::array<std::byte, kPageSize>{};
    std::memcpy(buffer.data(), page.data(), page.size());
    constexpr std::size_t checksum_offset = offsetof(PageHeader, checksum);
    std::memset(buffer.data() + checksum_offset, 0, sizeof(std::uint32_t));
    auto span = std::span<const std::byte>(buffer.data(), buffer.size());
    auto state = crc32c_extend(kCrc32cInit, span);
    return crc32c_finalize(state);
}

void update_page_checksum(std::span<std::byte> page)
{
    if (page.size() != kPageSize) {
        return;
    }
    auto checksum = compute_page_checksum(page);
    constexpr std::size_t checksum_offset = offsetof(PageHeader, checksum);
    std::memcpy(page.data() + checksum_offset, &checksum, sizeof(checksum));
}

bool verify_page_checksum(std::span<const std::byte> page)
{
    if (page.size() != kPageSize) {
        return false;
    }
    PageHeader header_copy{};
    std::memcpy(&header_copy, page.data(), sizeof(PageHeader));
    const auto expected = header_copy.checksum;
    auto checksum = compute_page_checksum(page);
    return checksum == expected;
}

std::uint32_t compute_wal_checksum(const WalRecordHeader& header, std::span<const std::byte> payload)
{
    auto copy = header;
    copy.checksum = 0U;
    auto header_bytes = std::as_bytes(std::span{&copy, 1});
    auto state = crc32c_extend(kCrc32cInit, header_bytes);
    state = crc32c_extend(state, payload);
    return crc32c_finalize(state);
}

void apply_wal_checksum(WalRecordHeader& header, std::span<const std::byte> payload)
{
    header.checksum = compute_wal_checksum(header, payload);
}

bool verify_wal_checksum(const WalRecordHeader& header, std::span<const std::byte> payload)
{
    return header.checksum == compute_wal_checksum(header, payload);
}

}  // namespace bored::storage
