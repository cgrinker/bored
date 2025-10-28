#pragma once

#include "bored/storage/wal_reader.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace bored::storage {

class TempResourceRegistry;
enum class TempResourcePurgeReason : std::uint8_t;

struct WalRecoveryRecord final {
    WalRecordHeader header{};
    std::vector<std::byte> payload{};
};

enum class WalRecoveredTransactionState : std::uint8_t {
    InFlight,
    Committed,
    Aborted
};

struct WalRecoveredTransaction final {
    std::uint64_t transaction_id = 0U;
    std::uint64_t first_lsn = 0U;
    std::uint64_t last_lsn = 0U;
    std::uint64_t commit_lsn = 0U;
    WalRecoveredTransactionState state = WalRecoveredTransactionState::InFlight;
    std::optional<WalRecoveryRecord> commit_record{};
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
    std::vector<WalRecoveredTransaction> transactions{};
    bool truncated_tail = false;
    std::uint64_t truncated_segment_id = 0U;
    std::uint64_t truncated_lsn = 0U;
    std::uint64_t next_transaction_id_high_water = 0U;
    std::uint64_t oldest_active_transaction_id = 0U;
    std::uint64_t oldest_active_commit_lsn = 0U;
};

class WalRecoveryDriver final {
public:
    WalRecoveryDriver(std::filesystem::path directory,
                      std::string file_prefix = "wal",
                      std::string file_extension = ".seg",
                      TempResourceRegistry* temp_resource_registry = nullptr);

    [[nodiscard]] std::error_code build_plan(WalRecoveryPlan& plan) const;

private:
    [[nodiscard]] std::error_code run_temp_cleanup() const;

    WalReader reader_;
    TempResourceRegistry* temp_resource_registry_ = nullptr;
};

}  // namespace bored::storage
