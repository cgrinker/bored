#pragma once

#include <cstddef>
#include <cstdint>
namespace bored::storage {

constexpr std::size_t kWalBlockSize = 4096;
constexpr std::uint32_t kWalMagic = 0xB0EDDA7A;
constexpr std::uint16_t kWalVersion = 1;
constexpr std::size_t kWalSegmentSize = 16 * 1024 * 1024;  // 16 MiB segments by default.

enum class WalRecordType : std::uint16_t {
    PageImage = 0,
    PageDelta = 1,
    Commit = 2,
    Abort = 3,
    Checkpoint = 4,
    TupleInsert = 5,
    TupleDelete = 6,
    TupleUpdate = 7,
    TupleBeforeImage = 8,
    TupleOverflowChunk = 9,
    TupleOverflowTruncate = 10,
    PageCompaction = 11,
    CatalogInsert = 12,
    CatalogDelete = 13,
    CatalogUpdate = 14,
    IndexSplit = 15,
    IndexMerge = 16,
    IndexBulkCheckpoint = 17
};

enum class WalRecordFlag : std::uint16_t {
    None = 0,
    HasPayload = 1 << 0,
    Compressed = 1 << 1
};

constexpr WalRecordFlag operator|(WalRecordFlag lhs, WalRecordFlag rhs)
{
    return static_cast<WalRecordFlag>(static_cast<std::uint16_t>(lhs) | static_cast<std::uint16_t>(rhs));
}

constexpr WalRecordFlag operator&(WalRecordFlag lhs, WalRecordFlag rhs)
{
    return static_cast<WalRecordFlag>(static_cast<std::uint16_t>(lhs) & static_cast<std::uint16_t>(rhs));
}

struct alignas(8) WalSegmentHeader final {
    std::uint32_t magic = kWalMagic;
    std::uint16_t version = kWalVersion;
    std::uint16_t reserved = 0U;
    std::uint64_t segment_id = 0U;
    std::uint64_t start_lsn = 0U;
    std::uint64_t end_lsn = 0U;
};

struct alignas(8) WalRecordHeader final {
    std::uint32_t total_length = sizeof(WalRecordHeader);
    std::uint16_t type = static_cast<std::uint16_t>(WalRecordType::PageImage);
    std::uint16_t flags = static_cast<std::uint16_t>(WalRecordFlag::None);
    std::uint64_t lsn = 0U;
    std::uint64_t prev_lsn = 0U;
    std::uint32_t page_id = 0U;
    std::uint32_t checksum = 0U;
};

constexpr std::size_t align_up_to_block(std::size_t value)
{
    return (value + (kWalBlockSize - 1U)) & ~(kWalBlockSize - 1U);
}

constexpr bool is_valid_segment_header(const WalSegmentHeader& header)
{
    return header.magic == kWalMagic && header.version == kWalVersion;
}

constexpr bool is_valid_record_header(const WalRecordHeader& header)
{
    return header.total_length >= sizeof(WalRecordHeader) && header.type <= static_cast<std::uint16_t>(WalRecordType::IndexBulkCheckpoint);
}

static_assert(sizeof(WalSegmentHeader) == 32, "WalSegmentHeader expected to be 32 bytes");
static_assert(sizeof(WalRecordHeader) == 32, "WalRecordHeader expected to be 32 bytes");
static_assert(alignof(WalSegmentHeader) == 8, "WalSegmentHeader alignment must be 8");
static_assert(alignof(WalRecordHeader) == 8, "WalRecordHeader alignment must be 8");
static_assert((kWalBlockSize & (kWalBlockSize - 1)) == 0, "Block size must be a power of two");
static_assert(kWalSegmentSize % kWalBlockSize == 0, "Segment size must be block aligned");

}  // namespace bored::storage
