#pragma once

#include <atomic>
#include <cstdint>

namespace bored::storage {

class WalDurabilityHorizon final {
public:
    void update(std::uint64_t commit_lsn,
                std::uint64_t oldest_active_commit_lsn,
                std::uint64_t commit_segment_id) noexcept
    {
        if (commit_lsn != 0U) {
            auto current_commit = last_commit_lsn_.load(std::memory_order_relaxed);
            while (commit_lsn > current_commit
                   && !last_commit_lsn_.compare_exchange_weak(current_commit,
                                                              commit_lsn,
                                                              std::memory_order_release,
                                                              std::memory_order_relaxed)) {
                // retry until observed commit horizon is up to date
            }
        }

        if (oldest_active_commit_lsn != 0U) {
            auto current_oldest = oldest_active_commit_lsn_.load(std::memory_order_relaxed);
            while (oldest_active_commit_lsn > current_oldest
                   && !oldest_active_commit_lsn_.compare_exchange_weak(current_oldest,
                                                                        oldest_active_commit_lsn,
                                                                        std::memory_order_release,
                                                                        std::memory_order_relaxed)) {
                // retry until oldest-active horizon advances
            }
        }

        if (commit_segment_id != 0U) {
            auto current_segment = last_commit_segment_id_.load(std::memory_order_relaxed);
            while (commit_segment_id > current_segment
                   && !last_commit_segment_id_.compare_exchange_weak(current_segment,
                                                                      commit_segment_id,
                                                                      std::memory_order_release,
                                                                      std::memory_order_relaxed)) {
                // retry until segment horizon advances
            }
        }
    }

    [[nodiscard]] std::uint64_t last_commit_lsn() const noexcept
    {
        return last_commit_lsn_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t oldest_active_commit_lsn() const noexcept
    {
        return oldest_active_commit_lsn_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t last_commit_segment_id() const noexcept
    {
        return last_commit_segment_id_.load(std::memory_order_acquire);
    }

    void reset() noexcept
    {
        last_commit_lsn_.store(0U, std::memory_order_release);
        oldest_active_commit_lsn_.store(0U, std::memory_order_release);
        last_commit_segment_id_.store(0U, std::memory_order_release);
    }

private:
    std::atomic<std::uint64_t> last_commit_lsn_{0U};
    std::atomic<std::uint64_t> oldest_active_commit_lsn_{0U};
    std::atomic<std::uint64_t> last_commit_segment_id_{0U};
};

}  // namespace bored::storage
