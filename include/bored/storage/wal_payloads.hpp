#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "bored/storage/index_btree_page.hpp"

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

struct alignas(8) WalCommitHeader final {
    std::uint64_t transaction_id = 0U;
    std::uint64_t commit_lsn = 0U;
    std::uint64_t next_transaction_id = 0U;
    std::uint64_t oldest_active_transaction_id = 0U;
    std::uint64_t oldest_active_commit_lsn = 0U;
};

struct alignas(8) WalTupleBeforeImageHeader final {
    WalTupleMeta meta{};
    std::uint32_t overflow_chunk_count = 0U;
    std::uint16_t previous_free_start = 0U;
    std::uint16_t previous_tuple_offset = 0U;
    std::uint64_t previous_page_lsn = 0U;
};

using WalTupleBeforeImageChunkView = WalOverflowTruncateChunkView;

struct WalTupleBeforeImageView final {
    WalTupleMeta meta{};
    std::span<const std::byte> tuple_payload{};
    std::vector<WalTupleBeforeImageChunkView> overflow_chunks{};
    std::uint64_t previous_page_lsn = 0U;
    std::uint16_t previous_free_start = 0U;
    std::uint16_t previous_tuple_offset = 0U;
};

struct alignas(8) WalCheckpointHeader final {
    std::uint64_t checkpoint_id = 0U;
    std::uint64_t redo_lsn = 0U;
    std::uint64_t undo_lsn = 0U;
    std::uint32_t dirty_page_count = 0U;
    std::uint32_t active_transaction_count = 0U;
};

struct alignas(8) WalCheckpointDirtyPageEntry final {
    std::uint32_t page_id = 0U;
    std::uint32_t reserved = 0U;
    std::uint64_t page_lsn = 0U;
};

struct alignas(8) WalCheckpointTxnEntry final {
    std::uint32_t transaction_id = 0U;
    std::uint32_t state = 0U;
    std::uint64_t last_lsn = 0U;
};

struct WalCheckpointView final {
    WalCheckpointHeader header{};
    std::vector<WalCheckpointDirtyPageEntry> dirty_pages{};
    std::vector<WalCheckpointTxnEntry> active_transactions{};
};

enum class WalIndexMaintenanceAction : std::uint32_t {
    None = 0,
    RefreshPointers = 1U << 0
};

constexpr WalIndexMaintenanceAction operator|(WalIndexMaintenanceAction lhs, WalIndexMaintenanceAction rhs)
{
    return static_cast<WalIndexMaintenanceAction>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
}

constexpr WalIndexMaintenanceAction operator&(WalIndexMaintenanceAction lhs, WalIndexMaintenanceAction rhs)
{
    return static_cast<WalIndexMaintenanceAction>(static_cast<std::uint32_t>(lhs) & static_cast<std::uint32_t>(rhs));
}

constexpr bool any(WalIndexMaintenanceAction value)
{
    return static_cast<std::uint32_t>(value) != 0U;
}

struct alignas(8) WalCompactionHeader final {
    std::uint32_t entry_count = 0U;
    std::uint32_t old_free_start = 0U;
    std::uint32_t new_free_start = 0U;
    std::uint32_t old_fragment_count = 0U;
    std::uint64_t reserved0 = 0U;
    std::uint64_t reserved1 = 0U;
};

struct alignas(8) WalCompactionEntry final {
    std::uint16_t slot_index = 0U;
    std::uint16_t reserved = 0U;
    std::uint32_t old_offset = 0U;
    std::uint32_t new_offset = 0U;
    std::uint32_t length = 0U;
    std::uint32_t index_action = static_cast<std::uint32_t>(WalIndexMaintenanceAction::None);
};

struct WalCompactionView final {
    WalCompactionHeader header{};
    std::vector<WalCompactionEntry> entries{};
};

enum class WalIndexSplitFlag : std::uint16_t {
    None = 0,
    Leaf = 1U << 0,
    Root = 1U << 1
};

constexpr WalIndexSplitFlag operator|(WalIndexSplitFlag lhs, WalIndexSplitFlag rhs)
{
    return static_cast<WalIndexSplitFlag>(static_cast<std::uint16_t>(lhs) | static_cast<std::uint16_t>(rhs));
}

constexpr WalIndexSplitFlag operator&(WalIndexSplitFlag lhs, WalIndexSplitFlag rhs)
{
    return static_cast<WalIndexSplitFlag>(static_cast<std::uint16_t>(lhs) & static_cast<std::uint16_t>(rhs));
}

constexpr bool any(WalIndexSplitFlag value)
{
    return static_cast<std::uint16_t>(value) != 0U;
}

enum class WalIndexMergeFlag : std::uint16_t {
    None = 0,
    Leaf = 1U << 0,
    Root = 1U << 1
};

constexpr WalIndexMergeFlag operator|(WalIndexMergeFlag lhs, WalIndexMergeFlag rhs)
{
    return static_cast<WalIndexMergeFlag>(static_cast<std::uint16_t>(lhs) | static_cast<std::uint16_t>(rhs));
}

constexpr WalIndexMergeFlag operator&(WalIndexMergeFlag lhs, WalIndexMergeFlag rhs)
{
    return static_cast<WalIndexMergeFlag>(static_cast<std::uint16_t>(lhs) & static_cast<std::uint16_t>(rhs));
}

constexpr bool any(WalIndexMergeFlag value)
{
    return static_cast<std::uint16_t>(value) != 0U;
}

enum class WalIndexBulkBuildStage : std::uint32_t {
    Unknown = 0,
    ScanHeap = 1,
    SortKeys = 2,
    WriteLeaves = 3,
    BuildUpperLevels = 4,
    Finalise = 5
};

enum class WalIndexBulkCheckpointFlag : std::uint32_t {
    None = 0,
    Completed = 1U << 0
};

constexpr WalIndexBulkCheckpointFlag operator|(WalIndexBulkCheckpointFlag lhs, WalIndexBulkCheckpointFlag rhs)
{
    return static_cast<WalIndexBulkCheckpointFlag>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
}

constexpr WalIndexBulkCheckpointFlag operator&(WalIndexBulkCheckpointFlag lhs, WalIndexBulkCheckpointFlag rhs)
{
    return static_cast<WalIndexBulkCheckpointFlag>(static_cast<std::uint32_t>(lhs) & static_cast<std::uint32_t>(rhs));
}

constexpr bool any(WalIndexBulkCheckpointFlag value)
{
    return static_cast<std::uint32_t>(value) != 0U;
}

struct alignas(8) WalIndexSplitHeader final {
    std::uint64_t index_id = 0U;
    std::uint32_t left_page_id = 0U;
    std::uint32_t right_page_id = 0U;
    std::uint32_t parent_page_id = 0U;
    std::uint32_t right_sibling_page_id = 0U;
    std::uint16_t level = 0U;
    std::uint16_t flags = static_cast<std::uint16_t>(WalIndexSplitFlag::None);
    std::uint16_t parent_insert_slot = 0U;
    std::uint16_t reserved0 = 0U;
    std::uint32_t pivot_key_length = 0U;
    std::uint32_t left_slot_count = 0U;
    std::uint32_t right_slot_count = 0U;
    std::uint32_t left_payload_length = 0U;
    std::uint32_t right_payload_length = 0U;
};

struct WalIndexSplitView final {
    WalIndexSplitHeader header{};
    std::span<const IndexBtreeSlotEntry> left_slots{};
    std::span<const std::byte> left_payload{};
    std::span<const IndexBtreeSlotEntry> right_slots{};
    std::span<const std::byte> right_payload{};
    std::span<const std::byte> pivot_key{};
};

struct alignas(8) WalIndexMergeHeader final {
    std::uint64_t index_id = 0U;
    std::uint32_t surviving_page_id = 0U;
    std::uint32_t removed_page_id = 0U;
    std::uint32_t parent_page_id = 0U;
    std::uint32_t new_right_sibling_page_id = 0U;
    std::uint16_t level = 0U;
    std::uint16_t flags = static_cast<std::uint16_t>(WalIndexMergeFlag::None);
    std::uint16_t parent_remove_slot = 0U;
    std::uint16_t reserved0 = 0U;
    std::uint32_t separator_key_length = 0U;
    std::uint32_t slot_count = 0U;
    std::uint32_t payload_length = 0U;
    std::uint32_t reserved1 = 0U;
};

struct WalIndexMergeView final {
    WalIndexMergeHeader header{};
    std::span<const IndexBtreeSlotEntry> slots{};
    std::span<const std::byte> payload{};
    std::span<const std::byte> separator_key{};
};

struct alignas(8) WalIndexBulkCheckpointHeader final {
    std::uint64_t index_id = 0U;
    std::uint64_t build_id = 0U;
    std::uint64_t processed_tuple_count = 0U;
    std::uint64_t last_heap_row_id = 0U;
    std::uint32_t stage = static_cast<std::uint32_t>(WalIndexBulkBuildStage::Unknown);
    std::uint32_t run_count = 0U;
    std::uint32_t pending_leaf_count = 0U;
    std::uint32_t pending_internal_count = 0U;
    std::uint32_t flags = static_cast<std::uint32_t>(WalIndexBulkCheckpointFlag::None);
    std::uint32_t reserved = 0U;
};

struct alignas(8) WalIndexBulkRunEntry final {
    std::uint32_t page_id = 0U;
    std::uint32_t level = 0U;
    std::uint32_t slot_count = 0U;
    std::uint32_t payload_length = 0U;
    std::uint64_t high_row_id = 0U;
};

struct WalIndexBulkCheckpointView final {
    WalIndexBulkCheckpointHeader header{};
    std::vector<WalIndexBulkRunEntry> pending_runs{};
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

std::size_t wal_tuple_before_image_payload_size(std::uint16_t tuple_length,
                                                std::span<const WalOverflowChunkMeta> chunk_metas);

std::size_t wal_overflow_truncate_payload_size(std::span<const WalOverflowChunkMeta> chunk_metas);

constexpr std::size_t wal_commit_payload_size()
{
    return sizeof(WalCommitHeader);
}

std::size_t wal_checkpoint_payload_size(std::size_t dirty_page_count, std::size_t active_transaction_count);

std::size_t wal_compaction_payload_size(std::size_t entry_count);

std::size_t wal_index_split_payload_size(std::size_t left_slot_count,
                                         std::size_t left_payload_length,
                                         std::size_t right_slot_count,
                                         std::size_t right_payload_length,
                                         std::size_t pivot_key_length);

std::size_t wal_index_merge_payload_size(std::size_t slot_count,
                                         std::size_t payload_length,
                                         std::size_t separator_key_length);

std::size_t wal_index_bulk_checkpoint_payload_size(std::size_t run_count);

bool encode_wal_tuple_insert(std::span<std::byte> buffer,
                             const WalTupleMeta& meta,
                             std::span<const std::byte> tuple_data);

bool encode_wal_tuple_delete(std::span<std::byte> buffer, const WalTupleMeta& meta);

bool encode_wal_tuple_update(std::span<std::byte> buffer,
                             const WalTupleUpdateMeta& meta,
                             std::span<const std::byte> new_tuple_data);

bool encode_wal_tuple_before_image(std::span<std::byte> buffer,
                                   const WalTupleMeta& meta,
                                   std::span<const std::byte> tuple_data,
                                   std::span<const WalOverflowChunkMeta> chunk_metas,
                                   std::span<const std::span<const std::byte>> chunk_payloads,
                                   std::uint64_t previous_page_lsn = 0U,
                                   std::uint16_t previous_free_start = 0U,
                                   std::uint16_t previous_tuple_offset = 0U);

std::optional<WalTupleMeta> decode_wal_tuple_meta(std::span<const std::byte> buffer);
std::optional<WalTupleUpdateMeta> decode_wal_tuple_update_meta(std::span<const std::byte> buffer);

std::optional<WalTupleBeforeImageView> decode_wal_tuple_before_image(std::span<const std::byte> buffer);

std::optional<WalOverflowChunkMeta> decode_wal_overflow_chunk_meta(std::span<const std::byte> buffer);
std::optional<WalOverflowTruncateMeta> decode_wal_overflow_truncate_meta(std::span<const std::byte> buffer);

std::optional<WalCheckpointView> decode_wal_checkpoint(std::span<const std::byte> buffer);

std::optional<WalCompactionView> decode_wal_compaction(std::span<const std::byte> buffer);

std::optional<WalIndexSplitView> decode_wal_index_split(std::span<const std::byte> buffer);

std::optional<WalIndexMergeView> decode_wal_index_merge(std::span<const std::byte> buffer);

std::optional<WalIndexBulkCheckpointView> decode_wal_index_bulk_checkpoint(std::span<const std::byte> buffer);

std::optional<WalCommitHeader> decode_wal_commit(std::span<const std::byte> buffer);

std::span<const std::byte> wal_tuple_payload(std::span<const std::byte> buffer, const WalTupleMeta& meta);
std::span<const std::byte> wal_tuple_update_payload(std::span<const std::byte> buffer, const WalTupleUpdateMeta& meta);

bool encode_wal_overflow_chunk(std::span<std::byte> buffer,
                               const WalOverflowChunkMeta& meta,
                               std::span<const std::byte> chunk_data);

bool encode_wal_overflow_truncate(std::span<std::byte> buffer,
                                  const WalOverflowTruncateMeta& meta,
                                  std::span<const WalOverflowChunkMeta> chunk_metas,
                                  std::span<const std::span<const std::byte>> chunk_payloads);

bool encode_wal_commit(std::span<std::byte> buffer, const WalCommitHeader& header);

bool encode_wal_checkpoint(std::span<std::byte> buffer,
                           const WalCheckpointHeader& header,
                           std::span<const WalCheckpointDirtyPageEntry> dirty_pages,
                           std::span<const WalCheckpointTxnEntry> active_transactions);

bool encode_wal_compaction(std::span<std::byte> buffer,
                           const WalCompactionHeader& header,
                           std::span<const WalCompactionEntry> entries);

bool encode_wal_index_split(std::span<std::byte> buffer,
                            const WalIndexSplitHeader& header,
                            std::span<const IndexBtreeSlotEntry> left_slots,
                            std::span<const std::byte> left_payload,
                            std::span<const IndexBtreeSlotEntry> right_slots,
                            std::span<const std::byte> right_payload,
                            std::span<const std::byte> pivot_key);

bool encode_wal_index_merge(std::span<std::byte> buffer,
                            const WalIndexMergeHeader& header,
                            std::span<const IndexBtreeSlotEntry> slots,
                            std::span<const std::byte> payload,
                            std::span<const std::byte> separator_key);

bool encode_wal_index_bulk_checkpoint(std::span<std::byte> buffer,
                                      const WalIndexBulkCheckpointHeader& header,
                                      std::span<const WalIndexBulkRunEntry> pending_runs);

std::span<const std::byte> wal_overflow_chunk_payload(std::span<const std::byte> buffer,
                                                      const WalOverflowChunkMeta& meta);

std::optional<std::vector<WalOverflowTruncateChunkView>> decode_wal_overflow_truncate_chunks(std::span<const std::byte> buffer,
                                                                                             const WalOverflowTruncateMeta& meta);

static_assert(sizeof(WalOverflowChunkMeta) == 32, "WalOverflowChunkMeta expected to be 32 bytes");
static_assert(alignof(WalOverflowChunkMeta) == 8, "WalOverflowChunkMeta requires 8-byte alignment");
static_assert(sizeof(WalOverflowTruncateMeta) == 32, "WalOverflowTruncateMeta expected to be 32 bytes");
static_assert(alignof(WalOverflowTruncateMeta) == 8, "WalOverflowTruncateMeta requires 8-byte alignment");
static_assert(sizeof(WalCommitHeader) == 40, "WalCommitHeader expected to be 40 bytes");
static_assert(alignof(WalCommitHeader) == 8, "WalCommitHeader requires 8-byte alignment");
static_assert(sizeof(WalTupleBeforeImageHeader) == 32, "WalTupleBeforeImageHeader expected to be 32 bytes");
static_assert(alignof(WalTupleBeforeImageHeader) == 8, "WalTupleBeforeImageHeader requires 8-byte alignment");
static_assert(sizeof(WalCheckpointHeader) == 32, "WalCheckpointHeader expected to be 32 bytes");
static_assert(alignof(WalCheckpointHeader) == 8, "WalCheckpointHeader requires 8-byte alignment");
static_assert(sizeof(WalCheckpointDirtyPageEntry) == 16, "WalCheckpointDirtyPageEntry expected to be 16 bytes");
static_assert(alignof(WalCheckpointDirtyPageEntry) == 8, "WalCheckpointDirtyPageEntry requires 8-byte alignment");
static_assert(sizeof(WalCheckpointTxnEntry) == 16, "WalCheckpointTxnEntry expected to be 16 bytes");
static_assert(alignof(WalCheckpointTxnEntry) == 8, "WalCheckpointTxnEntry requires 8-byte alignment");
static_assert(sizeof(WalCompactionHeader) == 32, "WalCompactionHeader expected to be 32 bytes");
static_assert(alignof(WalCompactionHeader) == 8, "WalCompactionHeader requires 8-byte alignment");
static_assert(sizeof(WalCompactionEntry) == 24, "WalCompactionEntry expected to be 24 bytes");
static_assert(alignof(WalCompactionEntry) == 8, "WalCompactionEntry requires 8-byte alignment");
static_assert(sizeof(WalIndexSplitHeader) == 56, "WalIndexSplitHeader expected to be 56 bytes");
static_assert(alignof(WalIndexSplitHeader) == 8, "WalIndexSplitHeader requires 8-byte alignment");
static_assert(sizeof(WalIndexMergeHeader) == 48, "WalIndexMergeHeader expected to be 48 bytes");
static_assert(alignof(WalIndexMergeHeader) == 8, "WalIndexMergeHeader requires 8-byte alignment");
static_assert(sizeof(WalIndexBulkCheckpointHeader) == 56, "WalIndexBulkCheckpointHeader expected to be 56 bytes");
static_assert(alignof(WalIndexBulkCheckpointHeader) == 8, "WalIndexBulkCheckpointHeader requires 8-byte alignment");
static_assert(sizeof(WalIndexBulkRunEntry) == 24, "WalIndexBulkRunEntry expected to be 24 bytes");
static_assert(alignof(WalIndexBulkRunEntry) == 8, "WalIndexBulkRunEntry requires 8-byte alignment");

}  // namespace bored::storage
