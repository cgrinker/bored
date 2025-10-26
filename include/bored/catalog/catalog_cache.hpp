#pragma once

#include "bored/catalog/catalog_relations.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>

namespace bored::catalog {

class CatalogCache final {
public:
    using TupleCallback = std::function<void(std::span<const std::byte> tuple)>;
    using RelationScanner = std::function<void(RelationId relation_id, const TupleCallback& callback)>;

    struct CachedTuple final {
        std::vector<std::byte> payload{};
    };

    struct CachedRelation final {
        std::vector<CachedTuple> tuples{};
        std::size_t total_bytes = 0U;
    };

    struct Config final {
        std::size_t max_relations = 32U;
        std::uint64_t retention_guard_epochs = 4U;
    };

    struct TelemetrySnapshot final {
        std::uint64_t hits = 0U;
        std::uint64_t misses = 0U;
        std::size_t relations = 0U;
        std::size_t total_bytes = 0U;
    };

    static CatalogCache& instance();

    void configure(const Config& config);
    void reset();
    void reset(const Config& config);

    [[nodiscard]] std::shared_ptr<const CachedRelation> materialize(RelationId relation_id,
                                                                    const RelationScanner& scanner);

    void invalidate(RelationId relation_id) noexcept;
    void invalidate_all() noexcept;

    [[nodiscard]] std::uint64_t epoch(RelationId relation_id) const noexcept;

    [[nodiscard]] TelemetrySnapshot telemetry() const noexcept;

private:
    struct RelationEntry final {
        std::shared_ptr<const CachedRelation> relation{};
        std::uint64_t epoch = 1U;
        bool dirty = true;
        std::uint64_t last_access_tick = 0U;
        std::uint64_t last_mutation_tick = 0U;
        std::size_t total_bytes = 0U;
    };

    CatalogCache();

    void evict_if_needed_locked();
    void evict_one_locked();
    [[nodiscard]] std::uint64_t next_access_tick_locked() noexcept;
    [[nodiscard]] std::uint64_t next_mutation_tick_locked() noexcept;

    Config config_{};
    mutable std::mutex mutex_{};
    std::unordered_map<std::uint64_t, RelationEntry> entries_{};
    std::uint64_t access_tick_ = 0U;
    std::uint64_t mutation_tick_ = 0U;
    std::atomic<std::uint64_t> hits_{0U};
    std::atomic<std::uint64_t> misses_{0U};
    std::size_t total_bytes_ = 0U;
};

}  // namespace bored::catalog
