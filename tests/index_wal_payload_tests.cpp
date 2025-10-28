#include "bored/storage/wal_payloads.hpp"

#include <algorithm>
#include <array>
#include <vector>

#include <catch2/catch_test_macros.hpp>

using bored::storage::IndexBtreeSlotEntry;
using bored::storage::WalIndexBulkBuildStage;
using bored::storage::WalIndexBulkCheckpointFlag;
using bored::storage::WalIndexBulkCheckpointHeader;
using bored::storage::WalIndexBulkRunEntry;
using bored::storage::WalIndexMergeFlag;
using bored::storage::WalIndexMergeHeader;
using bored::storage::WalIndexSplitFlag;
using bored::storage::WalIndexSplitHeader;

TEST_CASE("Index split WAL payload encodes and decodes", "[wal][index]")
{
    WalIndexSplitHeader header{};
    header.index_id = 42U;
    header.left_page_id = 100U;
    header.right_page_id = 200U;
    header.parent_page_id = 10U;
    header.right_sibling_page_id = 300U;
    header.level = 1U;
    header.flags = static_cast<std::uint16_t>(WalIndexSplitFlag::Leaf);
    header.parent_insert_slot = 2U;

    std::array<IndexBtreeSlotEntry, 2> left_slots{{
        IndexBtreeSlotEntry{.key_offset = 120U, .key_length = 12U},
        IndexBtreeSlotEntry{.key_offset = 140U, .key_length = 20U}
    }};

    std::array<IndexBtreeSlotEntry, 1> right_slots{{
        IndexBtreeSlotEntry{.key_offset = 64U, .key_length = static_cast<std::uint16_t>(0x8005)}
    }};

    std::array<std::byte, 16> left_payload{};
    std::array<std::byte, 24> right_payload{};
    for (std::size_t i = 0; i < left_payload.size(); ++i) {
        left_payload[i] = static_cast<std::byte>(i + 1U);
    }
    for (std::size_t i = 0; i < right_payload.size(); ++i) {
        right_payload[i] = static_cast<std::byte>(0xAAU + i);
    }

    std::array<std::byte, 5> pivot_key{};
    for (std::size_t i = 0; i < pivot_key.size(); ++i) {
        pivot_key[i] = static_cast<std::byte>(0x10U + i);
    }

    const auto buffer_size = bored::storage::wal_index_split_payload_size(left_slots.size(),
                                                                          left_payload.size(),
                                                                          right_slots.size(),
                                                                          right_payload.size(),
                                                                          pivot_key.size());
    std::vector<std::byte> buffer(buffer_size);

    REQUIRE(bored::storage::encode_wal_index_split(buffer,
                                                   header,
                                                   std::span<const IndexBtreeSlotEntry>(left_slots.data(), left_slots.size()),
                                                   std::span<const std::byte>(left_payload.data(), left_payload.size()),
                                                   std::span<const IndexBtreeSlotEntry>(right_slots.data(), right_slots.size()),
                                                   std::span<const std::byte>(right_payload.data(), right_payload.size()),
                                                   std::span<const std::byte>(pivot_key.data(), pivot_key.size())));

    auto decoded = bored::storage::decode_wal_index_split(buffer);
    REQUIRE(decoded);

    CHECK(decoded->header.index_id == header.index_id);
    CHECK(decoded->header.left_page_id == header.left_page_id);
    CHECK(decoded->header.right_page_id == header.right_page_id);
    CHECK(decoded->header.parent_page_id == header.parent_page_id);
    CHECK(decoded->header.right_sibling_page_id == header.right_sibling_page_id);
    CHECK(decoded->header.level == header.level);
    CHECK(decoded->header.flags == header.flags);
    CHECK(decoded->header.parent_insert_slot == header.parent_insert_slot);
    CHECK(decoded->header.left_slot_count == left_slots.size());
    CHECK(decoded->header.right_slot_count == right_slots.size());
    CHECK(decoded->header.left_payload_length == left_payload.size());
    CHECK(decoded->header.right_payload_length == right_payload.size());
    CHECK(decoded->header.pivot_key_length == pivot_key.size());

    REQUIRE(decoded->left_slots.size() == left_slots.size());
    REQUIRE(decoded->right_slots.size() == right_slots.size());
    REQUIRE(decoded->left_payload.size() == left_payload.size());
    REQUIRE(decoded->right_payload.size() == right_payload.size());
    REQUIRE(decoded->pivot_key.size() == pivot_key.size());

    for (std::size_t i = 0; i < left_slots.size(); ++i) {
        CHECK(decoded->left_slots[i].key_offset == left_slots[i].key_offset);
        CHECK(decoded->left_slots[i].key_length == left_slots[i].key_length);
    }
    for (std::size_t i = 0; i < right_slots.size(); ++i) {
        CHECK(decoded->right_slots[i].key_offset == right_slots[i].key_offset);
        CHECK(decoded->right_slots[i].key_length == right_slots[i].key_length);
    }
    CHECK(std::equal(decoded->left_payload.begin(), decoded->left_payload.end(), left_payload.begin(), left_payload.end()));
    CHECK(std::equal(decoded->right_payload.begin(), decoded->right_payload.end(), right_payload.begin(), right_payload.end()));
    CHECK(std::equal(decoded->pivot_key.begin(), decoded->pivot_key.end(), pivot_key.begin(), pivot_key.end()));
}

TEST_CASE("Index merge WAL payload encodes and decodes", "[wal][index]")
{
    WalIndexMergeHeader header{};
    header.index_id = 77U;
    header.surviving_page_id = 800U;
    header.removed_page_id = 801U;
    header.parent_page_id = 55U;
    header.new_right_sibling_page_id = 802U;
    header.level = 0U;
    header.flags = static_cast<std::uint16_t>(WalIndexMergeFlag::Leaf);
    header.parent_remove_slot = 9U;

    std::array<IndexBtreeSlotEntry, 3> slots{{
        IndexBtreeSlotEntry{.key_offset = 32U, .key_length = 8U},
        IndexBtreeSlotEntry{.key_offset = 48U, .key_length = 8U},
        IndexBtreeSlotEntry{.key_offset = 64U, .key_length = 8U}
    }};

    std::array<std::byte, 24> payload{};
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::byte>(0x40U + i);
    }

    std::array<std::byte, 6> separator{};
    for (std::size_t i = 0; i < separator.size(); ++i) {
        separator[i] = static_cast<std::byte>(0x70U + i);
    }

    const auto buffer_size = bored::storage::wal_index_merge_payload_size(slots.size(),
                                                                          payload.size(),
                                                                          separator.size());
    std::vector<std::byte> buffer(buffer_size);

    REQUIRE(bored::storage::encode_wal_index_merge(buffer,
                                                   header,
                                                   std::span<const IndexBtreeSlotEntry>(slots.data(), slots.size()),
                                                   std::span<const std::byte>(payload.data(), payload.size()),
                                                   std::span<const std::byte>(separator.data(), separator.size())));

    auto decoded = bored::storage::decode_wal_index_merge(buffer);
    REQUIRE(decoded);

    CHECK(decoded->header.index_id == header.index_id);
    CHECK(decoded->header.surviving_page_id == header.surviving_page_id);
    CHECK(decoded->header.removed_page_id == header.removed_page_id);
    CHECK(decoded->header.parent_page_id == header.parent_page_id);
    CHECK(decoded->header.new_right_sibling_page_id == header.new_right_sibling_page_id);
    CHECK(decoded->header.level == header.level);
    CHECK(decoded->header.flags == header.flags);
    CHECK(decoded->header.parent_remove_slot == header.parent_remove_slot);
    CHECK(decoded->header.slot_count == slots.size());
    CHECK(decoded->header.payload_length == payload.size());
    CHECK(decoded->header.separator_key_length == separator.size());

    REQUIRE(decoded->slots.size() == slots.size());
    REQUIRE(decoded->payload.size() == payload.size());
    REQUIRE(decoded->separator_key.size() == separator.size());

    for (std::size_t i = 0; i < slots.size(); ++i) {
        CHECK(decoded->slots[i].key_offset == slots[i].key_offset);
        CHECK(decoded->slots[i].key_length == slots[i].key_length);
    }
    CHECK(std::equal(decoded->payload.begin(), decoded->payload.end(), payload.begin(), payload.end()));
    CHECK(std::equal(decoded->separator_key.begin(), decoded->separator_key.end(), separator.begin(), separator.end()));
}

TEST_CASE("Index bulk checkpoint WAL payload encodes and decodes", "[wal][index]")
{
    WalIndexBulkCheckpointHeader header{};
    header.index_id = 901U;
    header.build_id = 123456U;
    header.processed_tuple_count = 42'000U;
    header.last_heap_row_id = 41'999U;
    header.stage = static_cast<std::uint32_t>(WalIndexBulkBuildStage::WriteLeaves);
    header.pending_leaf_count = 3U;
    header.pending_internal_count = 1U;
    header.flags = static_cast<std::uint32_t>(WalIndexBulkCheckpointFlag::Completed);

    std::array<WalIndexBulkRunEntry, 2> runs{{
        WalIndexBulkRunEntry{.page_id = 600U, .level = 0U, .slot_count = 48U, .payload_length = 1024U, .high_row_id = 10'000U},
        WalIndexBulkRunEntry{.page_id = 601U, .level = 1U, .slot_count = 12U, .payload_length = 512U, .high_row_id = 10'048U}
    }};

    const auto buffer_size = bored::storage::wal_index_bulk_checkpoint_payload_size(runs.size());
    std::vector<std::byte> buffer(buffer_size);

    REQUIRE(bored::storage::encode_wal_index_bulk_checkpoint(buffer,
                                                             header,
                                                             std::span<const WalIndexBulkRunEntry>(runs.data(), runs.size())));

    auto decoded = bored::storage::decode_wal_index_bulk_checkpoint(buffer);
    REQUIRE(decoded);

    CHECK(decoded->header.index_id == header.index_id);
    CHECK(decoded->header.build_id == header.build_id);
    CHECK(decoded->header.processed_tuple_count == header.processed_tuple_count);
    CHECK(decoded->header.last_heap_row_id == header.last_heap_row_id);
    CHECK(decoded->header.stage == header.stage);
    CHECK(decoded->header.pending_leaf_count == header.pending_leaf_count);
    CHECK(decoded->header.pending_internal_count == header.pending_internal_count);
    CHECK(decoded->header.flags == header.flags);
    CHECK(decoded->header.run_count == runs.size());

    REQUIRE(decoded->pending_runs.size() == runs.size());
    CHECK(std::equal(decoded->pending_runs.begin(), decoded->pending_runs.end(), runs.begin(), runs.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.page_id == rhs.page_id
            && lhs.level == rhs.level
            && lhs.slot_count == rhs.slot_count
            && lhs.payload_length == rhs.payload_length
            && lhs.high_row_id == rhs.high_row_id;
    }));
}
