#pragma once

#include "bored/storage/page_latch.hpp"

#include <cstdint>
#include <mutex>
#include <optional>
#include <system_error>
#include <thread>
#include <unordered_map>

namespace bored::storage {

class LockManager final {
public:
    struct Config final {
        bool enable_reentrancy = true;
    };

    LockManager();
    explicit LockManager(Config config);

    LockManager(const LockManager&) = delete;
    LockManager& operator=(const LockManager&) = delete;
    LockManager(LockManager&&) = delete;
    LockManager& operator=(LockManager&&) = delete;

    [[nodiscard]] std::error_code acquire(std::uint32_t page_id, PageLatchMode mode);
    void release(std::uint32_t page_id, PageLatchMode mode);

    [[nodiscard]] PageLatchCallbacks page_latch_callbacks();

private:
    struct HolderState final {
        std::uint32_t shared = 0U;
        std::uint32_t exclusive = 0U;
    };

    struct PageState final {
        std::uint32_t shared_total = 0U;
        std::optional<std::thread::id> exclusive_owner{};
        std::uint32_t exclusive_depth = 0U;
        std::unordered_map<std::thread::id, HolderState, std::hash<std::thread::id>> holders{};
    };

    [[nodiscard]] std::error_code acquire_shared(std::uint32_t page_id, PageState& state, HolderState& holder);
    [[nodiscard]] std::error_code acquire_exclusive(std::uint32_t page_id, PageState& state, HolderState& holder);
    void release_shared(std::uint32_t page_id, PageState& state, HolderState& holder);
    void release_exclusive(std::uint32_t page_id, PageState& state, HolderState& holder);
    void cleanup_if_unused(std::uint32_t page_id, PageState& state);

    Config config_{};
    std::mutex mutex_{};
    std::unordered_map<std::uint32_t, PageState> pages_{};
};

}  // namespace bored::storage
