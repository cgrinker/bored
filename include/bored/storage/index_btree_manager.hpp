#pragma once

#include "bored/storage/index_btree_page.hpp"
#include "bored/storage/index_comparator_registry.hpp"
#include "bored/storage/page_latch.hpp"

#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <system_error>

namespace bored::storage {

class FreeSpaceMap;
class TempResourceRegistry;

struct IndexInsertOptions final {
    bool allow_duplicates = true;
    bool infinite_key = false;
};

struct IndexInsertResult final {
    std::uint16_t slot_index = 0U;
    bool inserted = false;
    bool requires_split = false;
};

struct IndexDeleteResult final {
    std::size_t removed_count = 0U;
    bool page_empty = false;
};

struct IndexUpdateResult final {
    std::uint16_t slot_index = 0U;
    bool updated = false;
    bool requires_split = false;
};

struct IndexSearchResult final {
    bool found = false;
    std::uint16_t slot_index = 0U;
};

struct IndexBtreeManagerConfig final {
    const IndexComparatorEntry* comparator = nullptr;
    PageLatchCallbacks latch_callbacks{};
    TempResourceRegistry* temp_registry = nullptr;
    std::string scratch_tag{"index"};
};

class IndexBtreeManager final {
public:
    explicit IndexBtreeManager(IndexBtreeManagerConfig config);

    std::error_code insert_leaf(std::span<std::byte> page,
                                std::uint32_t page_id,
                                IndexBtreeTuplePointer tuple_pointer,
                                std::span<const std::byte> key,
                                std::uint64_t lsn,
                                IndexInsertOptions options,
                                IndexInsertResult& out_result);

    std::error_code delete_leaf(std::span<std::byte> page,
                                std::uint32_t page_id,
                                std::span<const std::byte> key,
                                IndexBtreeTuplePointer tuple_pointer,
                                bool remove_all_duplicates,
                                std::uint64_t lsn,
                                IndexDeleteResult& out_result);

    std::error_code update_leaf(std::span<std::byte> page,
                                std::uint32_t page_id,
                                std::uint16_t slot_index,
                                IndexBtreeTuplePointer tuple_pointer,
                                std::span<const std::byte> new_key,
                                std::uint64_t lsn,
                                IndexUpdateResult& out_result);

    IndexSearchResult find_leaf_slot(std::span<const std::byte> page,
                                     std::span<const std::byte> key,
                                     IndexBtreeTuplePointer tuple_pointer) const;

    [[nodiscard]] const IndexComparatorEntry* comparator() const noexcept
    {
        return comparator_;
    }

    [[nodiscard]] PageLatchCallbacks latch_callbacks() const noexcept
    {
        return config_.latch_callbacks;
    }

private:
    IndexBtreeManagerConfig config_{};
    const IndexComparatorEntry* comparator_ = nullptr;
    TempResourceRegistry* temp_registry_ = nullptr;
    std::filesystem::path scratch_directory_{};
    std::uint64_t scratch_sequence_ = 0U;

    std::error_code validate_leaf_page(std::span<const std::byte> page) const noexcept;
    std::error_code ensure_scratch_directory();
    void maybe_record_scratch(std::uint32_t page_id, std::span<const std::byte> payload);
};

std::error_code initialize_index_page(std::span<std::byte> page,
                                      std::uint32_t page_id,
                                      std::uint16_t level,
                                      bool is_leaf,
                                      std::uint64_t base_lsn = 0U,
                                      FreeSpaceMap* fsm = nullptr);

}  // namespace bored::storage
