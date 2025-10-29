#pragma once

#include "bored/catalog/catalog_relations.hpp"
#include "bored/storage/index_retention.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <functional>
#include <mutex>
#include <span>
#include <system_error>
#include <vector>

namespace bored::storage {

class IndexRetentionExecutor final {
public:
    struct Config final {
        std::function<std::error_code(const IndexRetentionCandidate&)> prune_callback{};
        std::function<std::optional<catalog::CatalogIndexDescriptor>(std::uint64_t)> index_lookup{};
        std::function<std::vector<std::uint32_t>(std::uint64_t)> leaf_page_enumerator{};
        std::function<std::error_code(std::uint64_t, std::uint32_t, std::span<std::byte>)> page_reader{};
        std::function<std::error_code(std::uint64_t, std::uint32_t, std::span<const std::byte>, std::uint64_t)> page_writer{};
        std::function<std::vector<WalCompactionEntry>(std::uint32_t, std::uint64_t)> compaction_lookup{};
    };

    IndexRetentionExecutor();
    explicit IndexRetentionExecutor(Config config);

    [[nodiscard]] std::error_code prune(const IndexRetentionCandidate& candidate);
    void reset();

    struct TelemetrySnapshot final {
        std::uint64_t runs = 0U;
        std::uint64_t successes = 0U;
        std::uint64_t failures = 0U;
        std::uint64_t skipped_candidates = 0U;
        std::uint64_t scanned_pages = 0U;
        std::uint64_t updated_pointers = 0U;
        std::uint64_t total_duration_ns = 0U;
        std::uint64_t last_duration_ns = 0U;
    };

    [[nodiscard]] TelemetrySnapshot telemetry_snapshot() const;

private:
    Config config_{};
    std::mutex mutex_{};
    TelemetrySnapshot telemetry_{};
    mutable std::mutex telemetry_mutex_{};

    void record_run(bool success,
                    bool skipped,
                    std::uint64_t scanned_pages,
                    std::uint64_t updated_pointers,
                    std::chrono::steady_clock::time_point start) noexcept;
};

}  // namespace bored::storage
