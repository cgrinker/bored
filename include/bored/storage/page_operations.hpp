#pragma once

#include "bored/storage/page_format.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace bored::storage {

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
                                      std::span<const std::byte> payload,
                                      std::uint64_t lsn,
                                      FreeSpaceMap* fsm = nullptr);

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

}  // namespace bored::storage
