#include "bored/storage/storage_diagnostics.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>

using namespace bored::storage;

namespace {

PageManagerTelemetrySnapshot make_page_manager(std::uint64_t seed)
{
    PageManagerTelemetrySnapshot snapshot{};
    snapshot.initialize.attempts = seed + 1U;
    snapshot.insert.attempts = seed + 2U;
    snapshot.update.attempts = seed + 3U;
    snapshot.remove.attempts = seed + 4U;
    snapshot.compact.attempts = seed + 5U;
    snapshot.shared_latch.attempts = seed + 6U;
    snapshot.exclusive_latch.attempts = seed + 7U;
    return snapshot;
}

CheckpointTelemetrySnapshot make_checkpoint(std::uint64_t seed)
{
    CheckpointTelemetrySnapshot snapshot{};
    snapshot.invocations = seed + 1U;
    snapshot.emitted_checkpoints = seed + 2U;
    snapshot.trigger_dirty = seed + 3U;
    snapshot.total_emit_duration_ns = (seed + 4U) * 10U;
    snapshot.last_checkpoint_id = seed + 5U;
    return snapshot;
}

WalRetentionTelemetrySnapshot make_retention(std::uint64_t seed)
{
    WalRetentionTelemetrySnapshot snapshot{};
    snapshot.invocations = seed + 1U;
    snapshot.pruned_segments = seed + 2U;
    snapshot.total_duration_ns = (seed + 3U) * 5U;
    return snapshot;
}

CatalogTelemetrySnapshot make_catalog(std::uint64_t seed)
{
    CatalogTelemetrySnapshot snapshot{};
    snapshot.cache_hits = (seed + 1U) * 10U;
    snapshot.cache_misses = (seed + 2U) * 4U;
    snapshot.cache_relations = static_cast<std::size_t>(seed + 3U);
    snapshot.cache_total_bytes = static_cast<std::size_t>((seed + 4U) * 512U);
    snapshot.published_batches = seed + 5U;
    snapshot.published_mutations = seed + 6U;
    snapshot.published_wal_records = seed + 7U;
    snapshot.publish_failures = seed + 8U;
    snapshot.aborted_batches = seed + 9U;
    snapshot.aborted_mutations = seed + 10U;
    return snapshot;
}

}  // namespace

TEST_CASE("collect_storage_diagnostics aggregates totals and details")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm_a", [] { return make_page_manager(1U); });
    registry.register_page_manager("pm_b", [] { return make_page_manager(5U); });
    registry.register_checkpoint_scheduler("ckpt_a", [] { return make_checkpoint(2U); });
    registry.register_checkpoint_scheduler("ckpt_b", [] { return make_checkpoint(4U); });
    registry.register_wal_retention("ret_a", [] { return make_retention(3U); });
    registry.register_catalog("cat_a", [] { return make_catalog(1U); });
    registry.register_catalog("cat_b", [] { return make_catalog(4U); });

    const StorageDiagnosticsOptions options{};
    const auto doc = collect_storage_diagnostics(registry, options);

    REQUIRE(doc.page_managers.details.size() == 2U);
    REQUIRE(doc.page_managers.total.initialize.attempts == ((1U + 1U) + (5U + 1U)));
    REQUIRE(doc.checkpoints.details.size() == 2U);
    REQUIRE(doc.checkpoints.total.emitted_checkpoints == ((2U + 2U) + (4U + 2U)));
    REQUIRE(doc.retention.details.size() == 1U);
    REQUIRE(doc.retention.total.pruned_segments == (3U + 2U));
    REQUIRE(doc.catalog.details.size() == 2U);
    REQUIRE(doc.catalog.total.cache_hits == ((1U + 1U) * 10U + (4U + 1U) * 10U));
    REQUIRE(doc.collected_at.time_since_epoch().count() != 0);

    REQUIRE(doc.page_managers.details.front().identifier == "pm_a");
    REQUIRE(doc.page_managers.details.back().identifier == "pm_b");
}

TEST_CASE("collect_storage_diagnostics honors detail options")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm", [] { return make_page_manager(1U); });
    registry.register_checkpoint_scheduler("ckpt", [] { return make_checkpoint(1U); });
    registry.register_wal_retention("ret", [] { return make_retention(1U); });
    registry.register_catalog("cat", [] { return make_catalog(2U); });

    StorageDiagnosticsOptions options{};
    options.include_page_manager_details = false;
    options.include_checkpoint_details = false;
    options.include_retention_details = false;
    options.include_catalog_details = false;

    const auto doc = collect_storage_diagnostics(registry, options);

    REQUIRE(doc.page_managers.details.empty());
    REQUIRE(doc.checkpoints.details.empty());
    REQUIRE(doc.retention.details.empty());
    REQUIRE(doc.catalog.details.empty());
}

TEST_CASE("storage_diagnostics_to_json serialises expected fields")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm", [] { return make_page_manager(3U); });
    registry.register_checkpoint_scheduler("ckpt", [] { return make_checkpoint(5U); });
    registry.register_wal_retention("ret", [] { return make_retention(7U); });
    registry.register_catalog("cat", [] { return make_catalog(4U); });

    const auto doc = collect_storage_diagnostics(registry);
    const auto json = storage_diagnostics_to_json(doc);

    REQUIRE(json.find("\"page_managers\"") != std::string::npos);
    REQUIRE(json.find("\"details\"") != std::string::npos);
    REQUIRE(json.find("\"pm\"") != std::string::npos);
    REQUIRE(json.find("\"checkpoints\"") != std::string::npos);
    REQUIRE(json.find("\"retention\"") != std::string::npos);
    REQUIRE(json.find("\"catalog\"") != std::string::npos);
    REQUIRE(json.find("\"pruned_segments\":9") != std::string::npos);
}
