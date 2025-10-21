#include "bored/storage/checksum.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_format.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <catch2/catch_test_macros.hpp>
#include <algorithm>
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

constexpr std::array<std::byte, 5> kTestPayload{std::byte{'a'}, std::byte{'l'}, std::byte{'p'}, std::byte{'h'}, std::byte{'a'}};

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
    auto chars = std::span<const char>{text.data(), text.size()};
    return std::as_bytes(chars);
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
    alignas(8) std::array<std::byte, bored::storage::kPageSize> buffer{};
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

TEST_CASE("Free space map tracks candidate pages")
{
    bored::storage::FreeSpaceMap fsm;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> buffer{};
    auto span = as_page_span(buffer);

    REQUIRE(bored::storage::initialize_page(span, PageType::Table, 19U, 1U, &fsm));
    REQUIRE(fsm.current_fragment_count(19U) == 0U);

    auto candidate = fsm.find_page_with_space(static_cast<std::uint16_t>(kTestPayload.size()));
    REQUIRE(candidate);
    REQUIRE(*candidate == 19U);

    auto slot = bored::storage::append_tuple(span, kTestPayload, 2U, &fsm);
    REQUIRE(slot);

    auto free_after_insert = fsm.current_free_bytes(19U);
    REQUIRE(free_after_insert < bored::storage::kPageSize - bored::storage::header_size());

    REQUIRE(bored::storage::delete_tuple(span, slot->index, 3U, &fsm));
    auto free_after_delete = fsm.current_free_bytes(19U);
    // Fragmentation keeps contiguous free space unchanged until vacuum rewrites the page.
    REQUIRE(free_after_delete == free_after_insert);
    auto& header = bored::storage::page_header(span);
    REQUIRE(header.fragment_count == 1U);
    REQUIRE(fsm.current_fragment_count(19U) == header.fragment_count);
}

TEST_CASE("Page compaction coalesces free space")
{
    bored::storage::FreeSpaceMap fsm;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> buffer{};
    auto span = as_page_span(buffer);

    REQUIRE(bored::storage::initialize_page(span, PageType::Table, 23U, 1U, &fsm));

    auto slot_a = bored::storage::append_tuple(span, to_payload("alpha"), 2U, &fsm);
    REQUIRE(slot_a);
    auto slot_b = bored::storage::append_tuple(span, to_payload("bravo"), 3U, &fsm);
    REQUIRE(slot_b);

    auto free_after_insert = fsm.current_free_bytes(23U);
    REQUIRE(free_after_insert < bored::storage::kPageSize - bored::storage::header_size());

    REQUIRE(bored::storage::delete_tuple(span, slot_a->index, 4U, &fsm));
    auto free_after_delete = fsm.current_free_bytes(23U);
    REQUIRE(free_after_delete == free_after_insert);
    REQUIRE(fsm.current_fragment_count(23U) == 1U);

    REQUIRE(bored::storage::compact_page(span, 5U, &fsm));
    auto free_after_compact = fsm.current_free_bytes(23U);
    REQUIRE(free_after_compact > free_after_delete);
    REQUIRE(fsm.current_fragment_count(23U) == 0U);

    auto surviving_tuple = bored::storage::read_tuple(as_page_span(buffer), slot_b->index);
    REQUIRE(surviving_tuple.size() == 5U);
    REQUIRE(std::string_view(reinterpret_cast<const char*>(surviving_tuple.data()), surviving_tuple.size()) == "bravo");
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
    record.type = static_cast<std::uint16_t>(WalRecordType::TupleInsert);
    REQUIRE(bored::storage::is_valid_record_header(record));
    REQUIRE(bored::storage::align_up_to_block(record.total_length) % bored::storage::kWalBlockSize == 0U);

    record.total_length = static_cast<std::uint32_t>(sizeof(WalRecordHeader) - 4U);
    REQUIRE_FALSE(bored::storage::is_valid_record_header(record));
}

TEST_CASE("CRC32C detects page corruption")
{
    alignas(8) std::array<std::byte, bored::storage::kPageSize> buffer{};
    auto span = as_page_span(buffer);
    REQUIRE(bored::storage::initialize_page(span, PageType::Table, 7U, 10U));

    auto slot = bored::storage::append_tuple(span, kTestPayload, 11U);
    REQUIRE(slot);

    bored::storage::update_page_checksum(span);
    REQUIRE(bored::storage::verify_page_checksum(as_page_span(buffer)));

    auto& header = bored::storage::page_header(span);
        header.checksum ^= 0xAAAA5555U;  // corrupt checksum
    REQUIRE_FALSE(bored::storage::verify_page_checksum(as_page_span(buffer)));
}

TEST_CASE("WAL checksum covers header and payload")
{
    bored::storage::WalRecordHeader header{};
    header.total_length = static_cast<std::uint32_t>(sizeof(bored::storage::WalRecordHeader) + kTestPayload.size());
    header.type = static_cast<std::uint16_t>(WalRecordType::PageImage);
    header.lsn = 101U;
    header.prev_lsn = 99U;
    header.page_id = 1234U;

    bored::storage::apply_wal_checksum(header, kTestPayload);
    REQUIRE(bored::storage::verify_wal_checksum(header, kTestPayload));

    header.page_id ^= 0x1U;
    REQUIRE_FALSE(bored::storage::verify_wal_checksum(header, kTestPayload));
}

TEST_CASE("WAL tuple insert payload round-trips")
{
    bored::storage::WalTupleMeta meta{};
    meta.page_id = 77U;
    meta.slot_index = 5U;
    meta.tuple_length = static_cast<std::uint16_t>(kTestPayload.size());
    meta.row_id = 0x1234'5678'9ABC'DEF0ULL;

    std::array<std::byte, 128> buffer{};
    auto span = std::span<std::byte>(buffer.data(), buffer.size());
    const auto required = bored::storage::wal_tuple_insert_payload_size(meta.tuple_length);
    auto target = span.subspan(0, required);

    auto payload_span = std::span<const std::byte>(kTestPayload);
    REQUIRE(bored::storage::encode_wal_tuple_insert(target, meta, payload_span));

    auto decoded = bored::storage::decode_wal_tuple_meta(target);
    REQUIRE(decoded);
    REQUIRE(decoded->page_id == meta.page_id);
    REQUIRE(decoded->slot_index == meta.slot_index);
    REQUIRE(decoded->tuple_length == meta.tuple_length);
    REQUIRE(decoded->row_id == meta.row_id);

    auto round_trip_payload = bored::storage::wal_tuple_payload(target, *decoded);
    REQUIRE(round_trip_payload.size() == kTestPayload.size());
    REQUIRE(std::equal(round_trip_payload.begin(), round_trip_payload.end(), kTestPayload.begin(), kTestPayload.end()));
}

TEST_CASE("WAL tuple update payload records previous length")
{
    bored::storage::WalTupleMeta base{};
    base.page_id = 11U;
    base.slot_index = 2U;
    base.tuple_length = static_cast<std::uint16_t>(kTestPayload.size());
    base.row_id = 88U;

    bored::storage::WalTupleUpdateMeta meta{};
    meta.base = base;
    meta.old_length = 21U;

    std::array<std::byte, 128> buffer{};
    auto span = std::span<std::byte>(buffer.data(), buffer.size());
    const auto required = bored::storage::wal_tuple_update_payload_size(base.tuple_length);
    auto target = span.subspan(0, required);

    auto payload_span = std::span<const std::byte>(kTestPayload);
    REQUIRE(bored::storage::encode_wal_tuple_update(target, meta, payload_span));

    auto decoded_meta = bored::storage::decode_wal_tuple_update_meta(target);
    REQUIRE(decoded_meta);
    REQUIRE(decoded_meta->base.page_id == base.page_id);
    REQUIRE(decoded_meta->base.slot_index == base.slot_index);
    REQUIRE(decoded_meta->base.tuple_length == base.tuple_length);
    REQUIRE(decoded_meta->old_length == meta.old_length);

    auto payload = bored::storage::wal_tuple_update_payload(target, *decoded_meta);
    REQUIRE(payload.size() == kTestPayload.size());
    REQUIRE(std::equal(payload.begin(), payload.end(), kTestPayload.begin(), kTestPayload.end()));
}
