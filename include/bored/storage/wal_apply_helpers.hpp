#pragma once

#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <span>
#include <system_error>

namespace bored::storage {

class FreeSpaceMap;

namespace wal {

bool page_contains_overflow(std::span<const std::byte> page);
void refresh_overflow_flag(std::span<std::byte> page);
bool page_already_applied(std::span<const std::byte> page, const WalRecordHeader& header);

std::error_code apply_tuple_insert(std::span<std::byte> page,
                                   const WalRecordHeader& header,
                                   const WalTupleMeta& meta,
                                   std::span<const std::byte> payload,
                                   FreeSpaceMap* fsm,
                                   bool force,
                                   std::uint64_t forced_lsn = 0U,
                                   std::uint16_t forced_free_start = 0U,
                                   std::uint16_t forced_tuple_offset = 0U);

std::error_code apply_tuple_delete(std::span<std::byte> page,
                                   const WalRecordHeader& header,
                                   const WalTupleMeta& meta,
                                   FreeSpaceMap* fsm,
                                   bool force);

std::error_code apply_tuple_update(std::span<std::byte> page,
                                   const WalRecordHeader& header,
                                   const WalTupleUpdateMeta& meta,
                                   std::span<const std::byte> payload,
                                   FreeSpaceMap* fsm);

}  // namespace wal

}  // namespace bored::storage
