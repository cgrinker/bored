#include "bored/storage/index_btree_manager.hpp"

#include "bored/storage/index_comparator_registry.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/temp_resource_registry.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstring>
#include <span>
#include <system_error>
#include <vector>

using bored::storage::IndexBtreeManager;
using bored::storage::IndexBtreeManagerConfig;
using bored::storage::IndexInsertOptions;
using bored::storage::IndexInsertResult;
using bored::storage::IndexDeleteResult;
using bored::storage::IndexUpdateResult;
using bored::storage::IndexSearchResult;
using bored::storage::IndexBtreeTuplePointer;
using bored::storage::IndexComparatorEntry;
using bored::storage::IndexBtreeSlotEntry;
using bored::storage::TempResourceRegistry;
using bored::storage::PageLatchCallbacks;
using bored::storage::PageLatchMode;
using bored::storage::PageHeader;
using bored::storage::PageType;
using bored::storage::initialize_index_page;
using bored::storage::page_header;
using bored::storage::index_page_is_leaf;
using bored::storage::kPageSize;

namespace {

using PageBuffer = std::array<std::byte, kPageSize>;

std::span<std::byte> as_span(PageBuffer& buffer)
{
    return {buffer.data(), buffer.size()};
}

std::span<const std::byte> as_span(const PageBuffer& buffer)
{
    return {buffer.data(), buffer.size()};
}

std::array<std::byte, sizeof(std::uint64_t)> make_key(std::uint64_t value)
{
    std::array<std::byte, sizeof(std::uint64_t)> key{};
    std::memcpy(key.data(), &value, sizeof(value));
    return key;
}

std::vector<std::uint64_t> read_keys(std::span<const std::byte> page)
{
    const auto& header = page_header(page);
    const auto slot_count = static_cast<std::size_t>(header.tuple_count);
    std::vector<std::uint64_t> keys;
    keys.reserve(slot_count);

    if (slot_count == 0U) {
        return keys;
    }

    const auto directory_begin = reinterpret_cast<const IndexBtreeSlotEntry*>(page.data() + header.free_end);
    for (std::size_t index = 0; index < slot_count; ++index) {
        const auto& slot = directory_begin[index];
        REQUIRE(slot.effective_length() == sizeof(std::uint64_t));
        std::uint64_t value = 0U;
        std::memcpy(&value, page.data() + slot.key_offset, sizeof(value));
        keys.push_back(value);
    }
    return keys;
}

std::vector<IndexBtreeTuplePointer> read_pointers(std::span<const std::byte> page)
{
    const auto& header = page_header(page);
    const auto slot_count = static_cast<std::size_t>(header.tuple_count);
    std::vector<IndexBtreeTuplePointer> pointers;
    pointers.reserve(slot_count);

    const auto pointer_size = sizeof(IndexBtreeTuplePointer);
    const auto directory_begin = reinterpret_cast<const IndexBtreeSlotEntry*>(page.data() + header.free_end);
    for (std::size_t index = 0; index < slot_count; ++index) {
        const auto& slot = directory_begin[index];
        REQUIRE(slot.key_offset >= pointer_size);
        const auto pointer_offset = static_cast<std::size_t>(slot.key_offset) - pointer_size;
        IndexBtreeTuplePointer pointer{};
        std::memcpy(&pointer, page.data() + pointer_offset, pointer_size);
        pointers.push_back(pointer);
    }
    return pointers;
}

const IndexComparatorEntry* require_comparator(std::string_view name)
{
    const auto* comparator = bored::storage::find_index_comparator(name);
    REQUIRE(comparator != nullptr);
    return comparator;
}

struct LatchTracker final {
    std::vector<std::pair<std::uint32_t, PageLatchMode>> acquires;
    std::vector<std::pair<std::uint32_t, PageLatchMode>> releases;

    PageLatchCallbacks callbacks()
    {
        PageLatchCallbacks cb{};
        cb.acquire = [this](std::uint32_t page_id, PageLatchMode mode) -> std::error_code {
            acquires.emplace_back(page_id, mode);
            return {};
        };
        cb.release = [this](std::uint32_t page_id, PageLatchMode mode) {
            releases.emplace_back(page_id, mode);
        };
        return cb;
    }
};

}  // namespace

TEST_CASE("initialize_index_page seeds header metadata")
{
    PageBuffer page{};
    auto span = as_span(page);

    auto ec = initialize_index_page(span, 222U, 0U, true, 0U, nullptr);
    REQUIRE_FALSE(ec);

    const auto& header = page_header(span);
    CHECK(header.page_id == 222U);
    CHECK(static_cast<PageType>(header.type) == PageType::Index);
    CHECK(header.tuple_count == 0U);
    CHECK(header.free_start == sizeof(PageHeader) + sizeof(bored::storage::IndexBtreePageHeader));
    CHECK(header.free_end == kPageSize);

    const auto& index = *reinterpret_cast<const bored::storage::IndexBtreePageHeader*>(span.data() + sizeof(PageHeader));
    CHECK(index_page_is_leaf(index));
    CHECK(index.level == 0U);
}

TEST_CASE("IndexBtreeManager inserts leaf entries in order")
{
    PageBuffer page{};
    auto span = as_span(page);
    REQUIRE_FALSE(initialize_index_page(span, 900U, 0U, true));

    TempResourceRegistry registry;
    LatchTracker tracker;

    IndexBtreeManager manager(IndexBtreeManagerConfig{
        .comparator = require_comparator("int64_ascending"),
        .latch_callbacks = tracker.callbacks(),
        .temp_registry = &registry,
        .scratch_tag = "itest"
    });

    const std::array<std::uint64_t, 3> values{50U, 10U, 30U};
    std::uint16_t slot_index = 0U;
    for (std::size_t i = 0; i < values.size(); ++i) {
        const auto key_bytes = make_key(values[i]);
        IndexInsertResult insert_result{};
        IndexBtreeTuplePointer pointer{};
        pointer.heap_page_id = 1000U + static_cast<std::uint32_t>(i);
        pointer.heap_slot_id = static_cast<std::uint16_t>(i + 1U);
        auto ec = manager.insert_leaf(span,
                                      900U,
                                      pointer,
                                      std::span<const std::byte>(key_bytes.data(), key_bytes.size()),
                                      i + 1U,
                                      IndexInsertOptions{},
                                      insert_result);
        REQUIRE_FALSE(ec);
        CHECK(insert_result.inserted);
        slot_index = insert_result.slot_index;
    }

    const auto keys = read_keys(as_span(page));
    REQUIRE(keys.size() == values.size());
    CHECK(keys == std::vector<std::uint64_t>{10U, 30U, 50U});

    const auto pointers = read_pointers(as_span(page));
    REQUIRE(pointers.size() == values.size());
    CHECK(pointers[0].heap_page_id == 1001U);
    CHECK(pointers[1].heap_page_id == 1002U);
    CHECK(pointers[2].heap_page_id == 1000U);

    CHECK_FALSE(registry.empty());
    REQUIRE(tracker.acquires.size() == values.size());
    REQUIRE(tracker.releases.size() == values.size());
}

TEST_CASE("IndexBtreeManager rejects duplicate keys when disallowed")
{
    PageBuffer page{};
    auto span = as_span(page);
    REQUIRE_FALSE(initialize_index_page(span, 901U, 0U, true));

    TempResourceRegistry registry;
    LatchTracker tracker;

    IndexBtreeManager manager(IndexBtreeManagerConfig{
        .comparator = require_comparator("int64_ascending"),
        .latch_callbacks = tracker.callbacks(),
        .temp_registry = &registry
    });

    IndexBtreeTuplePointer pointer{};
    pointer.heap_page_id = 88U;
    pointer.heap_slot_id = 4U;

    auto key_bytes = make_key(42U);
    IndexInsertResult first{};
    REQUIRE_FALSE(manager.insert_leaf(span,
                                      901U,
                                      pointer,
                                      std::span<const std::byte>(key_bytes.data(), key_bytes.size()),
                                      1U,
                                      IndexInsertOptions{},
                                      first));

    IndexInsertResult duplicate{};
    auto duplicate_error = manager.insert_leaf(span,
                                               901U,
                                               pointer,
                                               std::span<const std::byte>(key_bytes.data(), key_bytes.size()),
                                               2U,
                                               IndexInsertOptions{.allow_duplicates = false},
                                               duplicate);
    REQUIRE(duplicate_error == std::make_error_code(std::errc::file_exists));
    CHECK_FALSE(duplicate.inserted);
}

TEST_CASE("IndexBtreeManager signals split when page full")
{
    PageBuffer page{};
    auto span = as_span(page);
    REQUIRE_FALSE(initialize_index_page(span, 902U, 0U, true));

    TempResourceRegistry registry;
    IndexBtreeManager manager(IndexBtreeManagerConfig{
        .comparator = require_comparator("int64_ascending"),
        .temp_registry = &registry
    });

    std::uint64_t value = 0U;
    while (true) {
        IndexInsertResult result{};
        auto key = make_key(value);
        IndexBtreeTuplePointer pointer{};
        pointer.heap_page_id = 7000U + static_cast<std::uint32_t>(value);
        pointer.heap_slot_id = static_cast<std::uint16_t>(value % 0xFFFFU);
        auto ec = manager.insert_leaf(span,
                                      902U,
                                      pointer,
                                      std::span<const std::byte>(key.data(), key.size()),
                                      value + 1U,
                                      IndexInsertOptions{},
                                      result);
        if (ec) {
            CHECK(ec == std::make_error_code(std::errc::no_buffer_space));
            CHECK(result.requires_split);
            break;
        }
        ++value;
    }
}

TEST_CASE("IndexBtreeManager delete removes matching entry")
{
    PageBuffer page{};
    auto span = as_span(page);
    REQUIRE_FALSE(initialize_index_page(span, 903U, 0U, true));

    TempResourceRegistry registry;
    IndexBtreeManager manager(IndexBtreeManagerConfig{
        .comparator = require_comparator("int64_ascending"),
        .temp_registry = &registry
    });

    const std::array<std::uint64_t, 3> keys_to_insert{11U, 22U, 33U};
    std::vector<IndexBtreeTuplePointer> pointers;
    for (std::size_t i = 0; i < keys_to_insert.size(); ++i) {
        IndexInsertResult insert_result{};
        auto key = make_key(keys_to_insert[i]);
        IndexBtreeTuplePointer pointer{};
        pointer.heap_page_id = 2000U + static_cast<std::uint32_t>(i);
        pointer.heap_slot_id = static_cast<std::uint16_t>(i + 10U);
        REQUIRE_FALSE(manager.insert_leaf(span,
                                          903U,
                                          pointer,
                                          std::span<const std::byte>(key.data(), key.size()),
                                          10U + i,
                                          IndexInsertOptions{},
                                          insert_result));
        pointers.push_back(pointer);
    }

    auto delete_key_bytes = make_key(22U);
    IndexDeleteResult delete_result{};
    auto delete_error = manager.delete_leaf(span,
                                            903U,
                                            std::span<const std::byte>(delete_key_bytes.data(), delete_key_bytes.size()),
                                            pointers[1],
                                            false,
                                            99U,
                                            delete_result);
    REQUIRE_FALSE(delete_error);
    CHECK(delete_result.removed_count == 1U);
    CHECK_FALSE(delete_result.page_empty);

    const auto remaining_keys = read_keys(as_span(page));
    CHECK(remaining_keys == std::vector<std::uint64_t>{11U, 33U});
}

TEST_CASE("IndexBtreeManager update relocates entry when key changes")
{
    PageBuffer page{};
    auto span = as_span(page);
    REQUIRE_FALSE(initialize_index_page(span, 904U, 0U, true));

    TempResourceRegistry registry;
    IndexBtreeManager manager(IndexBtreeManagerConfig{
        .comparator = require_comparator("int64_ascending"),
        .temp_registry = &registry
    });

    std::array<IndexBtreeTuplePointer, 3> pointers{};
    for (std::size_t i = 0; i < pointers.size(); ++i) {
        IndexInsertResult result{};
        auto key = make_key((i + 1U) * 10U);
        IndexBtreeTuplePointer pointer{};
        pointer.heap_page_id = 3000U + static_cast<std::uint32_t>(i);
        pointer.heap_slot_id = static_cast<std::uint16_t>(i);
        REQUIRE_FALSE(manager.insert_leaf(span,
                                          904U,
                                          pointer,
                                          std::span<const std::byte>(key.data(), key.size()),
                                          5U + i,
                                          IndexInsertOptions{},
                                          result));
        pointers[i] = pointer;
    }

    IndexUpdateResult update_result{};
    auto new_key = make_key(25U);
    auto update_error = manager.update_leaf(span,
                                            904U,
                                            0U,
                                            pointers[0],
                                            std::span<const std::byte>(new_key.data(), new_key.size()),
                                            77U,
                                            update_result);
    REQUIRE_FALSE(update_error);
    CHECK(update_result.updated);
    CHECK(update_result.slot_index == 1U);

    const auto keys = read_keys(as_span(page));
    CHECK(keys == std::vector<std::uint64_t>{20U, 25U, 30U});
}

TEST_CASE("IndexBtreeManager find locates stored tuple")
{
    PageBuffer page{};
    auto span = as_span(page);
    REQUIRE_FALSE(initialize_index_page(span, 905U, 0U, true));

    TempResourceRegistry registry;
    IndexBtreeManager manager(IndexBtreeManagerConfig{
        .comparator = require_comparator("int64_ascending"),
        .temp_registry = &registry
    });

    IndexBtreeTuplePointer pointer{};
    pointer.heap_page_id = 555U;
    pointer.heap_slot_id = 12U;
    auto key = make_key(1234U);
    IndexInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_leaf(span,
                                      905U,
                                      pointer,
                                      std::span<const std::byte>(key.data(), key.size()),
                                      1U,
                                      IndexInsertOptions{},
                                      insert_result));

    auto search = manager.find_leaf_slot(as_span(page),
                                         std::span<const std::byte>(key.data(), key.size()),
                                         pointer);
    CHECK(search.found);
    CHECK(search.slot_index == 0U);

    auto missing_key = make_key(4321U);
    auto missing = manager.find_leaf_slot(as_span(page),
                                          std::span<const std::byte>(missing_key.data(), missing_key.size()),
                                          pointer);
    CHECK_FALSE(missing.found);
}