#pragma once

#include "bored/storage/page_format.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_recovery.hpp"

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace bored::storage {

class FreeSpaceMap;

class WalReplayContext final {
public:
    explicit WalReplayContext(PageType default_page_type = PageType::Table, FreeSpaceMap* fsm = nullptr);

    void set_page(std::uint32_t page_id, std::span<const std::byte> image);
    [[nodiscard]] std::span<std::byte> get_page(std::uint32_t page_id);
    void set_free_space_map(FreeSpaceMap* fsm) noexcept;
    [[nodiscard]] FreeSpaceMap* free_space_map() const noexcept;
    void record_index_metadata(std::span<const WalCompactionEntry> entries);
    [[nodiscard]] const std::vector<WalCompactionEntry>& index_metadata() const noexcept;

private:
    PageType default_page_type_;
    std::unordered_map<std::uint32_t, std::array<std::byte, kPageSize>> pages_;
    FreeSpaceMap* free_space_map_ = nullptr;
    std::vector<WalCompactionEntry> index_metadata_events_{};
};

class WalReplayer final {
public:
    explicit WalReplayer(WalReplayContext& context);

    [[nodiscard]] std::error_code apply_redo(const WalRecoveryPlan& plan);
    [[nodiscard]] std::error_code apply_undo(const WalRecoveryPlan& plan);
    [[nodiscard]] std::optional<WalRecordType> last_undo_type() const noexcept;

private:
    [[nodiscard]] std::error_code apply_redo_record(const WalRecoveryRecord& record);
    [[nodiscard]] std::error_code apply_undo_record(const WalRecoveryRecord& record);

    WalReplayContext& context_;
    std::optional<WalRecordType> last_undo_type_{};
};

}  // namespace bored::storage
