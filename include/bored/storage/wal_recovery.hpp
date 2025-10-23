#pragma once

#include "bored/storage/wal_reader.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>
#include <vector>

namespace bored::storage {

struct WalRecoveryRecord final {
    WalRecordHeader header{};
    std::vector<std::byte> payload{};
};

struct WalUndoSpan final {
    std::uint32_t owner_page_id = 0U;
    std::size_t offset = 0U;
    std::size_t count = 0U;
};

struct WalRecoveryPlan final {
    std::vector<WalRecoveryRecord> redo{};
    std::vector<WalRecoveryRecord> undo{};
    std::vector<WalUndoSpan> undo_spans{};
    bool truncated_tail = false;
    std::uint64_t truncated_segment_id = 0U;
    std::uint64_t truncated_lsn = 0U;
};

class WalRecoveryDriver final {
public:
    WalRecoveryDriver(std::filesystem::path directory,
                      std::string file_prefix = "wal",
                      std::string file_extension = ".seg");

    [[nodiscard]] std::error_code build_plan(WalRecoveryPlan& plan) const;

private:
    WalReader reader_;
};

}  // namespace bored::storage
