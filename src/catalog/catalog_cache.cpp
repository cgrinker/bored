#include "bored/catalog/catalog_cache.hpp"

#include <algorithm>

namespace bored::catalog {

namespace {

constexpr std::size_t kDefaultMaxRelations = 32U;
constexpr std::uint64_t kDefaultRetentionGuard = 4U;

}  // namespace

CatalogCache::CatalogCache()
    : config_{kDefaultMaxRelations, kDefaultRetentionGuard}
{}

CatalogCache& CatalogCache::instance()
{
    static CatalogCache cache;
    return cache;
}

void CatalogCache::configure(const Config& config)
{
    std::lock_guard guard(mutex_);
    config_ = config;
    if (config_.max_relations == 0U) {
        config_.max_relations = kDefaultMaxRelations;
    }
    if (config_.retention_guard_epochs == 0U) {
        config_.retention_guard_epochs = kDefaultRetentionGuard;
    }
    evict_if_needed_locked();
}

void CatalogCache::reset(const Config& config)
{
    std::lock_guard guard(mutex_);
    config_ = config;
    if (config_.max_relations == 0U) {
        config_.max_relations = kDefaultMaxRelations;
    }
    if (config_.retention_guard_epochs == 0U) {
        config_.retention_guard_epochs = kDefaultRetentionGuard;
    }
    entries_.clear();
    access_tick_ = 0U;
    mutation_tick_ = 0U;
    total_bytes_ = 0U;
    hits_.store(0U, std::memory_order_relaxed);
    misses_.store(0U, std::memory_order_relaxed);
}

std::shared_ptr<const CatalogCache::CachedRelation> CatalogCache::materialize(RelationId relation_id,
                                                                             const RelationScanner& scanner)
{
    if (!relation_id.is_valid() || !scanner) {
        return {};
    }

    {
        std::lock_guard guard(mutex_);
        auto it = entries_.find(relation_id.value);
        if (it != entries_.end() && !it->second.dirty && it->second.relation) {
            it->second.last_access_tick = next_access_tick_locked();
            hits_.fetch_add(1U, std::memory_order_relaxed);
            return it->second.relation;
        }

        misses_.fetch_add(1U, std::memory_order_relaxed);
        if (it == entries_.end()) {
            RelationEntry entry;
            entry.epoch = 1U;
            entry.dirty = true;
            entry.last_mutation_tick = next_mutation_tick_locked();
            it = entries_.emplace(relation_id.value, std::move(entry)).first;
        }
        it->second.dirty = true;
    }

    auto relation = std::make_shared<CachedRelation>();
    scanner(relation_id, [&relation](std::span<const std::byte> tuple) {
        CachedTuple entry;
        entry.payload.assign(tuple.begin(), tuple.end());
        relation->total_bytes += entry.payload.size();
        relation->tuples.push_back(std::move(entry));
    });

    {
        std::lock_guard guard(mutex_);
        auto& entry = entries_[relation_id.value];
        if (!entry.dirty && entry.relation) {
            entry.last_access_tick = next_access_tick_locked();
            return entry.relation;
        }

        if (total_bytes_ >= entry.total_bytes) {
            total_bytes_ -= entry.total_bytes;
        } else {
            total_bytes_ = 0U;
        }
        entry.relation = std::move(relation);
        entry.dirty = false;
        entry.last_access_tick = next_access_tick_locked();
        entry.total_bytes = entry.relation ? entry.relation->total_bytes : 0U;
        entry.last_mutation_tick = next_mutation_tick_locked();
        total_bytes_ += entry.total_bytes;
        if (entry.epoch == 0U) {
            entry.epoch = 1U;
        }
        evict_if_needed_locked();
        return entry.relation;
    }
}

void CatalogCache::invalidate(RelationId relation_id) noexcept
{
    if (!relation_id.is_valid()) {
        return;
    }

    std::lock_guard guard(mutex_);
    auto& entry = entries_[relation_id.value];
    if (entry.epoch == 0U) {
        entry.epoch = 1U;
    } else {
        ++entry.epoch;
    }
    entry.dirty = true;
        if (total_bytes_ >= entry.total_bytes) {
            total_bytes_ -= entry.total_bytes;
        } else {
            total_bytes_ = 0U;
        }
    entry.total_bytes = 0U;
    entry.relation.reset();
    entry.last_mutation_tick = next_mutation_tick_locked();
}

void CatalogCache::invalidate_all() noexcept
{
    std::lock_guard guard(mutex_);
    for (auto& [_, entry] : entries_) {
        if (entry.epoch == 0U) {
            entry.epoch = 1U;
        } else {
            ++entry.epoch;
        }
        entry.dirty = true;
        if (total_bytes_ >= entry.total_bytes) {
            total_bytes_ -= entry.total_bytes;
        } else {
            total_bytes_ = 0U;
        }
        entry.total_bytes = 0U;
        entry.relation.reset();
        entry.last_mutation_tick = next_mutation_tick_locked();
    }
}

std::uint64_t CatalogCache::epoch(RelationId relation_id) const noexcept
{
    if (!relation_id.is_valid()) {
        return 0U;
    }

    std::lock_guard guard(mutex_);
    auto it = entries_.find(relation_id.value);
    if (it == entries_.end()) {
        return 1U;
    }
    return it->second.epoch;
}

CatalogCache::TelemetrySnapshot CatalogCache::telemetry() const noexcept
{
    std::lock_guard guard(mutex_);
    TelemetrySnapshot snapshot{};
    snapshot.hits = hits_.load(std::memory_order_relaxed);
    snapshot.misses = misses_.load(std::memory_order_relaxed);
    snapshot.relations = entries_.size();
    snapshot.total_bytes = total_bytes_;
    return snapshot;
}

void CatalogCache::evict_if_needed_locked()
{
    while (entries_.size() > config_.max_relations) {
        evict_one_locked();
        if (entries_.size() <= config_.max_relations) {
            break;
        }
    }
}

void CatalogCache::evict_one_locked()
{
    if (entries_.empty()) {
        return;
    }

    auto best = entries_.end();
    auto fallback = entries_.end();
    const auto guard_threshold = (mutation_tick_ > config_.retention_guard_epochs)
        ? (mutation_tick_ - config_.retention_guard_epochs)
        : 0U;

    for (auto it = entries_.begin(); it != entries_.end(); ++it) {
        const auto& entry = it->second;
        if (!entry.relation) {
            fallback = it;
            continue;
        }
        if (entry.last_mutation_tick <= guard_threshold) {
            if (best == entries_.end() || entry.last_access_tick < best->second.last_access_tick) {
                best = it;
            }
        } else if (fallback == entries_.end() || entry.last_access_tick < fallback->second.last_access_tick) {
            fallback = it;
        }
    }

    auto target = (best != entries_.end()) ? best : fallback;
    if (target == entries_.end()) {
        return;
    }

    if (total_bytes_ >= target->second.total_bytes) {
        total_bytes_ -= target->second.total_bytes;
    } else {
        total_bytes_ = 0U;
    }
    entries_.erase(target);
}

std::uint64_t CatalogCache::next_access_tick_locked() noexcept
{
    return ++access_tick_;
}

std::uint64_t CatalogCache::next_mutation_tick_locked() noexcept
{
    return ++mutation_tick_;
}

}  // namespace bored::catalog
