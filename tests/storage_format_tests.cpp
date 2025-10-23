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
#include <vector>
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

TEST_CASE("WAL overflow chunk payload round-trips")
{
    bored::storage::WalOverflowChunkMeta meta{};
    meta.owner.page_id = 501U;
    meta.owner.slot_index = 19U;
    meta.owner.tuple_length = 4096U;
    meta.owner.row_id = 0xABCD'0123'4567'89EFULL;
    meta.overflow_page_id = 0xDEADBEEFU;
    meta.next_overflow_page_id = 0xCAFEBABEU;
    meta.chunk_offset = 64U;
    meta.chunk_length = 48U;
    meta.chunk_index = 2U;
    meta.flags = static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainEnd);

    std::array<std::byte, 256> buffer{};
    auto span = std::span<std::byte>(buffer.data(), buffer.size());
    const auto required = bored::storage::wal_overflow_chunk_payload_size(meta.chunk_length);
    auto target = span.subspan(0, required);

    std::array<std::byte, 48> chunk{};
    for (std::size_t index = 0; index < chunk.size(); ++index) {
        chunk[index] = std::byte{static_cast<unsigned char>(index)};
    }

    auto chunk_view = std::span<const std::byte>(chunk.data(), chunk.size());
    REQUIRE(bored::storage::encode_wal_overflow_chunk(target, meta, chunk_view));

    auto decoded_meta = bored::storage::decode_wal_overflow_chunk_meta(target);
    REQUIRE(decoded_meta);
    CHECK(decoded_meta->owner.page_id == meta.owner.page_id);
    CHECK(decoded_meta->owner.slot_index == meta.owner.slot_index);
    CHECK(decoded_meta->owner.tuple_length == meta.owner.tuple_length);
    CHECK(decoded_meta->owner.row_id == meta.owner.row_id);
    CHECK(decoded_meta->overflow_page_id == meta.overflow_page_id);
    CHECK(decoded_meta->next_overflow_page_id == meta.next_overflow_page_id);
    CHECK(decoded_meta->chunk_offset == meta.chunk_offset);
    CHECK(decoded_meta->chunk_length == meta.chunk_length);
    CHECK(decoded_meta->chunk_index == meta.chunk_index);
    CHECK(decoded_meta->flags == meta.flags);

    auto payload = bored::storage::wal_overflow_chunk_payload(target, *decoded_meta);
    REQUIRE(payload.size() == chunk.size());
    REQUIRE(std::equal(payload.begin(), payload.end(), chunk.begin(), chunk.end()));
}

TEST_CASE("WAL overflow truncate payload round-trips")
{
    bored::storage::WalOverflowTruncateMeta meta{};
    meta.owner.page_id = 777U;
    meta.owner.slot_index = 31U;
    meta.owner.tuple_length = 1024U;
    meta.owner.row_id = 0x1111'2222'3333'4444ULL;
    meta.first_overflow_page_id = 0xFEEDC0DEU;
    meta.released_page_count = 3U;

    std::vector<bored::storage::WalOverflowChunkMeta> chunk_metas;
    std::vector<std::vector<std::byte>> chunk_payloads;
    for (std::uint16_t index = 0; index < meta.released_page_count; ++index) {
        bored::storage::WalOverflowChunkMeta chunk_meta{};
        chunk_meta.owner = meta.owner;
        chunk_meta.overflow_page_id = meta.first_overflow_page_id + index;
        chunk_meta.next_overflow_page_id = (index + 1U < meta.released_page_count) ? (meta.first_overflow_page_id + index + 1U) : 0U;
        chunk_meta.chunk_offset = static_cast<std::uint16_t>(index * 200U);
        chunk_meta.chunk_length = static_cast<std::uint16_t>(100U + index * 10U);
        chunk_meta.chunk_index = index;
        chunk_meta.flags = (index == 0U ? static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainStart) : 0U) |
                           (index + 1U == meta.released_page_count ? static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainEnd) : 0U);
        chunk_metas.push_back(chunk_meta);

        std::vector<std::byte> payload(chunk_meta.chunk_length);
        for (std::size_t byte_index = 0; byte_index < payload.size(); ++byte_index) {
            payload[byte_index] = static_cast<std::byte>(index + byte_index);
        }
        chunk_payloads.push_back(payload);
    }

    std::vector<std::span<const std::byte>> payload_spans;
    payload_spans.reserve(chunk_payloads.size());
    for (const auto& payload : chunk_payloads) {
        payload_spans.emplace_back(payload.data(), payload.size());
    }

    const auto required = bored::storage::wal_overflow_truncate_payload_size(std::span<const bored::storage::WalOverflowChunkMeta>(chunk_metas.data(), chunk_metas.size()));
    std::vector<std::byte> buffer(required);
    auto target = std::span<std::byte>(buffer.data(), buffer.size());

    REQUIRE(bored::storage::encode_wal_overflow_truncate(target,
                                                         meta,
                                                         std::span<const bored::storage::WalOverflowChunkMeta>(chunk_metas.data(), chunk_metas.size()),
                                                         std::span<const std::span<const std::byte>>(payload_spans.data(), payload_spans.size())));

    auto decoded = bored::storage::decode_wal_overflow_truncate_meta(target);
    REQUIRE(decoded);
    CHECK(decoded->owner.page_id == meta.owner.page_id);
    CHECK(decoded->owner.slot_index == meta.owner.slot_index);
    CHECK(decoded->owner.tuple_length == meta.owner.tuple_length);
    CHECK(decoded->owner.row_id == meta.owner.row_id);
    CHECK(decoded->first_overflow_page_id == meta.first_overflow_page_id);
    CHECK(decoded->released_page_count == meta.released_page_count);

    auto decoded_chunks = bored::storage::decode_wal_overflow_truncate_chunks(target, *decoded);
    REQUIRE(decoded_chunks);
    REQUIRE(decoded_chunks->size() == chunk_metas.size());
    for (std::size_t index = 0; index < chunk_metas.size(); ++index) {
        const auto& expected_meta = chunk_metas[index];
        const auto& expected_payload = chunk_payloads[index];
        const auto& view = decoded_chunks->at(index);
        CHECK(view.meta.overflow_page_id == expected_meta.overflow_page_id);
        CHECK(view.meta.next_overflow_page_id == expected_meta.next_overflow_page_id);
        CHECK(view.meta.chunk_offset == expected_meta.chunk_offset);
        CHECK(view.meta.chunk_length == expected_meta.chunk_length);
        CHECK(view.meta.chunk_index == expected_meta.chunk_index);
        CHECK(view.meta.flags == expected_meta.flags);
        REQUIRE(view.payload.size() == expected_payload.size());
        REQUIRE(std::equal(view.payload.begin(), view.payload.end(), expected_payload.begin(), expected_payload.end()));
    }
}
