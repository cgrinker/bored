#pragma once

#include "bored/storage/page_format.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace bored::storage {

struct WalOverflowChunkMeta;
struct WalOverflowTruncateMeta;

constexpr std::uint32_t kOverflowTupleMagic = 0x4F564552;  // 'OVER'

struct OverflowTupleHeader final {
    std::uint32_t magic = kOverflowTupleMagic;
    std::uint32_t logical_length = 0U;
    std::uint32_t first_overflow_page_id = 0U;
    std::uint32_t inline_length = 0U;
    std::uint16_t chunk_count = 0U;
    std::uint16_t flags = 0U;
};

constexpr std::size_t overflow_tuple_header_size()
{
    return sizeof(OverflowTupleHeader);
}

enum class TupleFlag : std::uint16_t {
    None = 0U,
    HasOverflow = 1U << 0U
};

inline constexpr bool any(TupleFlag flag) noexcept
{
    return static_cast<std::uint16_t>(flag) != 0U;
}

inline constexpr TupleFlag operator|(TupleFlag lhs, TupleFlag rhs) noexcept
{
    return static_cast<TupleFlag>(static_cast<std::uint16_t>(lhs) | static_cast<std::uint16_t>(rhs));
}

inline constexpr TupleFlag operator&(TupleFlag lhs, TupleFlag rhs) noexcept
{
    return static_cast<TupleFlag>(static_cast<std::uint16_t>(lhs) & static_cast<std::uint16_t>(rhs));
}

inline constexpr TupleFlag& operator|=(TupleFlag& lhs, TupleFlag rhs) noexcept
{
    lhs = lhs | rhs;
    return lhs;
}

struct TupleHeader final {
    txn::TransactionId created_transaction_id = 0U;
    txn::TransactionId deleted_transaction_id = 0U;
    std::uint64_t undo_next_lsn = 0U;
    std::uint16_t flags = 0U;
    std::uint16_t reserved = 0U;
    std::uint32_t reserved_padding = 0U;
};

constexpr std::size_t tuple_header_size()
{
    return sizeof(TupleHeader);
}

constexpr std::size_t tuple_storage_length(std::size_t payload_length)
{
    return tuple_header_size() + payload_length;
}

std::optional<TupleHeader> read_tuple_header(std::span<const std::byte> page, std::uint16_t slot_index);
bool write_tuple_header(std::span<std::byte> page, std::uint16_t slot_index, const TupleHeader& header);
std::span<const std::byte> read_tuple_storage(std::span<const std::byte> page, std::uint16_t slot_index);
bool is_overflow_tuple(std::span<const std::byte> tuple);
std::optional<OverflowTupleHeader> parse_overflow_tuple(std::span<const std::byte> tuple);
std::span<const std::byte> overflow_tuple_inline_payload(std::span<const std::byte> tuple,
                                                         const OverflowTupleHeader& header);


struct TupleSlot final {
    std::uint16_t index = 0U;
    std::uint16_t offset = 0U;
    std::uint16_t length = 0U;
};

struct AppendTuplePlan final {
    std::uint16_t slot_index = 0U;
    std::uint16_t write_offset = 0U;
    std::uint16_t tuple_length = 0U;
    bool reuses_slot = false;
};

class FreeSpaceMap;

PageHeader& page_header(std::span<std::byte> page);
const PageHeader& page_header(std::span<const std::byte> page);

std::span<SlotPointer> slot_directory(std::span<std::byte> page);
std::span<const SlotPointer> slot_directory(std::span<const std::byte> page);

bool initialize_page(std::span<std::byte> page,
                     PageType type,
                     std::uint32_t page_id,
                     std::uint64_t lsn = 0U,
                     FreeSpaceMap* fsm = nullptr);

std::optional<TupleSlot> append_tuple(std::span<std::byte> page,
                                      const TupleHeader& header,
                                      std::span<const std::byte> payload,
                                      std::uint64_t lsn,
                                      FreeSpaceMap* fsm = nullptr);

inline std::optional<TupleSlot> append_tuple(std::span<std::byte> page,
                                             std::span<const std::byte> payload,
                                             std::uint64_t lsn,
                                             FreeSpaceMap* fsm = nullptr)
{
    return append_tuple(page, TupleHeader{}, payload, lsn, fsm);
}

std::optional<AppendTuplePlan> prepare_append_tuple(std::span<const std::byte> page,
                                                    std::size_t payload_length);

bool delete_tuple(std::span<std::byte> page,
                  std::uint16_t slot_index,
                  std::uint64_t lsn,
                  FreeSpaceMap* fsm = nullptr);

std::span<const std::byte> read_tuple(std::span<const std::byte> page, std::uint16_t slot_index);

bool compact_page(std::span<std::byte> page,
                  std::uint64_t lsn,
                  FreeSpaceMap* fsm = nullptr);

void sync_free_space(FreeSpaceMap& fsm, std::span<const std::byte> page);

bool write_overflow_chunk(std::span<std::byte> page,
                          const WalOverflowChunkMeta& meta,
                          std::span<const std::byte> payload,
                          std::uint64_t lsn,
                          FreeSpaceMap* fsm = nullptr);

bool clear_overflow_page(std::span<std::byte> page,
                         std::uint32_t page_id,
                         std::uint64_t lsn,
                         FreeSpaceMap* fsm = nullptr);

std::optional<WalOverflowChunkMeta> read_overflow_chunk_meta(std::span<const std::byte> page);

std::span<const std::byte> overflow_chunk_payload(std::span<const std::byte> page,
                                                   const WalOverflowChunkMeta& meta);

}  // namespace bored::storage
