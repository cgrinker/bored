#pragma once

#include "bored/storage/page_latch.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_writer.hpp"

#include <filesystem>
#include <functional>
#include <memory>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace bored::storage {

class PageManager final {
public:
    struct Config final {
        std::uint32_t overflow_page_start = 1U << 20;  // default high range to reduce collisions
        std::size_t overflow_inline_prefix = 256U;
        PageLatchCallbacks latch_callbacks{};
        std::function<void(const WalCompactionEntry&)> index_metadata_callback{};
    };

    struct TupleInsertResult final {
        TupleSlot slot{};
        WalAppendResult wal{};
        bool used_overflow = false;
        std::size_t logical_length = 0U;
        std::size_t inline_length = 0U;
        std::vector<std::uint32_t> overflow_page_ids{};
    };

    struct TupleDeleteResult final {
        WalAppendResult wal{};
    };

    struct TupleUpdateResult final {
        TupleSlot slot{};
        WalAppendResult wal{};
        std::uint16_t old_length = 0U;
        bool used_overflow = false;
        std::size_t logical_length = 0U;
        std::size_t inline_length = 0U;
        std::vector<std::uint32_t> overflow_page_ids{};
    };

    struct PageCompactionResult final {
        WalAppendResult compaction_wal{};
        bool performed = false;
        std::vector<WalCompactionEntry> relocations{};
    };

    PageManager(FreeSpaceMap* fsm, std::shared_ptr<WalWriter> wal_writer, Config config = {});

    PageManager(const PageManager&) = delete;
    PageManager& operator=(const PageManager&) = delete;
    PageManager(PageManager&&) = delete;
    PageManager& operator=(PageManager&&) = delete;

    [[nodiscard]] std::error_code initialize_page(std::span<std::byte> page,
                                                  PageType type,
                                                  std::uint32_t page_id,
                                                  std::uint64_t base_lsn = 0U) const;

    [[nodiscard]] std::error_code insert_tuple(std::span<std::byte> page,
                                               std::span<const std::byte> payload,
                                               std::uint64_t row_id,
                                               TupleInsertResult& out_result) const;

    [[nodiscard]] std::error_code delete_tuple(std::span<std::byte> page,
                                               std::uint16_t slot_index,
                                               std::uint64_t row_id,
                                               TupleDeleteResult& out_result) const;

    [[nodiscard]] std::error_code update_tuple(std::span<std::byte> page,
                                               std::uint16_t slot_index,
                                               std::span<const std::byte> new_payload,
                                               std::uint64_t row_id,
                                               TupleUpdateResult& out_result) const;

    [[nodiscard]] std::error_code compact_page(std::span<std::byte> page,
                                               PageCompactionResult& out_result) const;

    [[nodiscard]] std::error_code flush_wal() const;
    [[nodiscard]] std::error_code close_wal() const;

    [[nodiscard]] std::error_code persist_free_space_map(const std::filesystem::path& path) const;
    [[nodiscard]] std::error_code load_free_space_map(const std::filesystem::path& path) const;

    [[nodiscard]] std::shared_ptr<WalWriter> wal_writer() const noexcept;

private:
    FreeSpaceMap* fsm_ = nullptr;
    std::shared_ptr<WalWriter> wal_writer_{};
    Config config_{};
    mutable std::uint32_t next_overflow_page_id_ = 0U;
    struct OverflowChunkCacheEntry final {
        WalOverflowChunkMeta meta{};
        std::vector<std::byte> payload{};
    };
    mutable std::unordered_map<std::uint32_t, OverflowChunkCacheEntry> overflow_cache_{};

    [[nodiscard]] std::uint32_t allocate_overflow_page_id() const;
    [[nodiscard]] std::error_code build_overflow_truncate_payload(const WalTupleMeta& owner_meta,
                                                                  const OverflowTupleHeader& header,
                                                                  std::vector<WalOverflowChunkMeta>& chunk_metas,
                                                                  std::vector<std::vector<std::byte>>& chunk_payloads,
                                                                  WalOverflowTruncateMeta& truncate_meta) const;
};

}  // namespace bored::storage
