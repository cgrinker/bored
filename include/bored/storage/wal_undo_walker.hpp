#pragma once

#include "bored/storage/wal_recovery.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace bored::storage {

struct WalUndoWorkItem final {
    std::uint32_t owner_page_id = 0U;
    std::span<const WalRecoveryRecord> records{};
    std::vector<std::uint32_t> overflow_page_ids{};
};

class WalUndoWalker final {
public:
    explicit WalUndoWalker(const WalRecoveryPlan& plan) noexcept;

    void reset() noexcept;
    [[nodiscard]] std::optional<WalUndoWorkItem> next();

private:
    const WalRecoveryPlan* plan_ = nullptr;
    std::size_t span_index_ = 0U;
};

}  // namespace bored::storage
