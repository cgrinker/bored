#include "bored/storage/page_format.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_format.hpp"

#include <catch2/catch_test_macros.hpp>
#include <array>
#include <span>
#include <string_view>

using bored::storage::PageFlag;
using bored::storage::PageHeader;
using bored::storage::PageType;
using bored::storage::SlotPointer;
using bored::storage::WalRecordHeader;
using bored::storage::WalRecordType;
using bored::storage::WalSegmentHeader;

namespace {

std::span<std::byte> as_page_span(std::array<std::byte, bored::storage::kPageSize>& buffer)
{
    return {buffer.data(), buffer.size()};
}

std::span<const std::byte> as_page_span(const std::array<std::byte, bored::storage::kPageSize>& buffer)
{
    return {buffer.data(), buffer.size()};
}

std::span<const std::byte> to_payload(std::string_view text)
{
    return std::as_bytes(std::span{text.data(), text.size()});
}

}  // namespace

TEST_CASE("Empty page reports maximum free space")
{
    PageHeader header{};
    header.type = static_cast<std::uint8_t>(PageType::Table);
    header.free_start = static_cast<std::uint16_t>(bored::storage::header_size());
    header.free_end = static_cast<std::uint16_t>(bored::storage::kPageSize);

    REQUIRE(bored::storage::is_valid(header));
    REQUIRE(bored::storage::compute_free_bytes(header) == bored::storage::kPageSize - bored::storage::header_size());
    REQUIRE(bored::storage::max_slot_count() >= 2000);
}

TEST_CASE("Page flags tracking works")
{
    PageHeader header{};
    header.flags = static_cast<std::uint16_t>(PageFlag::Dirty);

    REQUIRE(bored::storage::has_flag(header, PageFlag::Dirty));
    REQUIRE_FALSE(bored::storage::has_flag(header, PageFlag::HasOverflow));
}

TEST_CASE("Page lifecycle supports inserts and deletes")
{
    std::array<std::byte, bored::storage::kPageSize> buffer{};
    auto span = as_page_span(buffer);

    REQUIRE(bored::storage::initialize_page(span, PageType::Table, 42U, 1U));
    auto& header = bored::storage::page_header(span);
    REQUIRE(header.page_id == 42U);
    REQUIRE(header.tuple_count == 0U);

    auto slot0 = bored::storage::append_tuple(span, to_payload("alpha"), 2U);
    REQUIRE(slot0);
    auto slot1 = bored::storage::append_tuple(span, to_payload("beta"), 3U);
    REQUIRE(slot1);

    auto view0 = bored::storage::read_tuple(as_page_span(buffer), slot0->index);
    REQUIRE(std::string_view(reinterpret_cast<const char*>(view0.data()), view0.size()) == "alpha");

    REQUIRE(bored::storage::delete_tuple(span, slot0->index, 4U));
    REQUIRE(header.fragment_count == 1U);

    auto slot2 = bored::storage::append_tuple(span, to_payload("gamma"), 5U);
    REQUIRE(slot2);
    REQUIRE(slot2->index == slot0->index);
    REQUIRE(header.fragment_count == 0U);
}

TEST_CASE("Wal headers validate expectations")
{
    WalSegmentHeader segment{};
    segment.segment_id = 7U;
    segment.start_lsn = 128U;
    segment.end_lsn = 256U;

    REQUIRE(bored::storage::is_valid_segment_header(segment));

    WalRecordHeader record{};
    record.total_length = static_cast<std::uint32_t>(sizeof(WalRecordHeader) + 128U);
    record.type = static_cast<std::uint16_t>(WalRecordType::Commit);

    REQUIRE(bored::storage::is_valid_record_header(record));
    REQUIRE(bored::storage::align_up_to_block(record.total_length) % bored::storage::kWalBlockSize == 0U);

    record.total_length = static_cast<std::uint32_t>(sizeof(WalRecordHeader) - 4U);
    REQUIRE_FALSE(bored::storage::is_valid_record_header(record));
}
