#pragma once

#include "bored/storage/storage_telemetry_registry.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <unordered_map>

namespace bored::storage {

struct IndexRetentionCandidate final {
    std::uint64_t index_id = 0U;
    std::uint32_t page_id = 0U;
    std::uint16_t level = 0U;
    std::uint64_t prune_lsn = 0U;
    std::chrono::steady_clock::time_point scheduled_at{};
};

struct IndexRetentionStats final {
    std::uint64_t scanned_candidates = 0U;
    std::uint64_t dispatched_candidates = 0U;
    std::uint64_t skipped_candidates = 0U;
    std::uint64_t pruned_candidates = 0U;
};

class IndexRetentionManager final {
public:
    struct Config final {
        std::chrono::milliseconds min_checkpoint_interval{std::chrono::minutes{5}};
        std::chrono::milliseconds retention_window{std::chrono::hours{1}};
        std::size_t max_dispatch_batch = 128U;
        std::size_t max_pending_candidates = 2048U;
        StorageTelemetryRegistry* telemetry_registry = nullptr;
        std::string telemetry_identifier{};
    };

    using DispatchHook = std::function<std::error_code(std::span<const IndexRetentionCandidate>, IndexRetentionStats&)>;

    IndexRetentionManager();
    explicit IndexRetentionManager(Config config);
    IndexRetentionManager(Config config, DispatchHook dispatch);
    ~IndexRetentionManager();

    IndexRetentionManager(const IndexRetentionManager&) = delete;
    IndexRetentionManager& operator=(const IndexRetentionManager&) = delete;
    IndexRetentionManager(IndexRetentionManager&&) = delete;
    IndexRetentionManager& operator=(IndexRetentionManager&&) = delete;

    void schedule_candidate(IndexRetentionCandidate candidate);
    void clear_index(std::uint64_t index_id);
    void reset();

    [[nodiscard]] std::error_code apply_checkpoint(std::chrono::steady_clock::time_point now,
                                                   std::uint64_t checkpoint_lsn,
                                                   IndexRetentionStats* stats = nullptr);

    [[nodiscard]] Config config() const noexcept;
    [[nodiscard]] IndexRetentionTelemetrySnapshot telemetry_snapshot() const;

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

    using CandidateEntry = std::pair<CandidateKey, IndexRetentionCandidate>;

    void register_telemetry();
    void unregister_telemetry();

    [[nodiscard]] std::chrono::steady_clock::time_point now() const noexcept;

    Config config_{};
    DispatchHook dispatch_{};
    mutable std::mutex mutex_{};
    std::unordered_map<CandidateKey, IndexRetentionCandidate, CandidateKeyHasher> pending_{};
    IndexRetentionTelemetrySnapshot telemetry_{};
    std::chrono::steady_clock::time_point last_checkpoint_time_{};
    bool has_checkpoint_time_ = false;
};

}  // namespace bored::storage
