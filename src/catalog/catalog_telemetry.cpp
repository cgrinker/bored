#include "bored/catalog/catalog_telemetry.hpp"

#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_mutator.hpp"

#include <utility>

namespace bored::catalog {

storage::CatalogTelemetrySnapshot collect_catalog_telemetry()
{
    storage::CatalogTelemetrySnapshot snapshot{};

    const auto cache_snapshot = CatalogCache::instance().telemetry();
    snapshot.cache_hits = cache_snapshot.hits;
    snapshot.cache_misses = cache_snapshot.misses;
    snapshot.cache_relations = cache_snapshot.relations;
    snapshot.cache_total_bytes = cache_snapshot.total_bytes;

    const auto mutation_snapshot = CatalogMutator::telemetry();
    snapshot.published_batches = mutation_snapshot.published_batches;
    snapshot.published_mutations = mutation_snapshot.published_mutations;
    snapshot.published_wal_records = mutation_snapshot.published_wal_records;
    snapshot.publish_failures = mutation_snapshot.publish_failures;
    snapshot.aborted_batches = mutation_snapshot.aborted_batches;
    snapshot.aborted_mutations = mutation_snapshot.aborted_mutations;

    return snapshot;
}

void register_catalog_telemetry(storage::StorageTelemetryRegistry& registry, std::string identifier)
{
    registry.register_catalog(std::move(identifier), [] {
        return collect_catalog_telemetry();
    });
}

void unregister_catalog_telemetry(storage::StorageTelemetryRegistry& registry, const std::string& identifier)
{
    registry.unregister_catalog(identifier);
}

}  // namespace bored::catalog
