#pragma once

#include "bored/storage/index_retention.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <span>
#include <system_error>
#include <unordered_map>

namespace bored::storage {

class IndexRetentionPruner final {
public:
    struct Config final {
        std::function<std::error_code(const IndexRetentionCandidate&)> prune_callback{};
        std::chrono::milliseconds state_retention{std::chrono::minutes{30}};
    };

    IndexRetentionPruner();
    explicit IndexRetentionPruner(Config config);

    [[nodiscard]] std::error_code dispatch(std::span<const IndexRetentionCandidate> candidates,
                                           IndexRetentionStats& stats);

    void reset_state();

private:
    struct CandidateKey final {
        std::uint64_t index_id = 0U;
        std::uint32_t page_id = 0U;
        std::uint16_t level = 0U;

        [[nodiscard]] bool operator==(const CandidateKey& other) const noexcept
        {
            return index_id == other.index_id
                && page_id == other.page_id
                && level == other.level;
        }
    };

    struct CandidateKeyHasher final {
        [[nodiscard]] std::size_t operator()(const CandidateKey& key) const noexcept
        {
            std::size_t hash = std::hash<std::uint64_t>{}(key.index_id);
            hash ^= std::hash<std::uint32_t>{}(key.page_id) + 0x9e3779b9 + (hash << 6U) + (hash >> 2U);
            hash ^= std::hash<std::uint16_t>{}(key.level) + 0x9e3779b9 + (hash << 6U) + (hash >> 2U);
            return hash;
        }
    };

    struct CandidateState final {
        std::uint64_t last_prune_lsn = 0U;
        std::chrono::steady_clock::time_point last_pruned_at{};
        std::chrono::steady_clock::time_point last_seen_at{};
    };

    void evict_stale(std::chrono::steady_clock::time_point now);

    Config config_{};
    std::mutex mutex_{};
    std::unordered_map<CandidateKey, CandidateState, CandidateKeyHasher> states_{};
};

}  // namespace bored::storage
