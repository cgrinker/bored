#pragma once

#include "bored/storage/page_format.hpp"
#include "bored/storage/wal_recovery.hpp"

#include <array>
#include <cstdint>
#include <span>
#include <system_error>
#include <unordered_map>

namespace bored::storage {

class WalReplayContext final {
public:
    explicit WalReplayContext(PageType default_page_type = PageType::Table);

    void set_page(std::uint32_t page_id, std::span<const std::byte> image);
    [[nodiscard]] std::span<std::byte> get_page(std::uint32_t page_id);

private:
    PageType default_page_type_;
    std::unordered_map<std::uint32_t, std::array<std::byte, kPageSize>> pages_;
};

class WalReplayer final {
public:
    explicit WalReplayer(WalReplayContext& context);

    [[nodiscard]] std::error_code apply_redo(const WalRecoveryPlan& plan);
    [[nodiscard]] std::error_code apply_undo(const WalRecoveryPlan& plan);

private:
    [[nodiscard]] std::error_code apply_redo_record(const WalRecoveryRecord& record);
    [[nodiscard]] std::error_code apply_undo_record(const WalRecoveryRecord& record);

    WalReplayContext& context_;
};

}  // namespace bored::storage
