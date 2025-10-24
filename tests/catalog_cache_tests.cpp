#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <span>
#include <unordered_map>

using namespace bored::catalog;

namespace {

CatalogCache::RelationScanner make_scanner(std::unordered_map<std::uint64_t, std::size_t>& counters)
{
    return [&counters](RelationId relation_id, const CatalogCache::TupleCallback& callback) {
        ++counters[relation_id.value];
        std::array<std::byte, 8U> payload{};
        payload[0] = std::byte{static_cast<unsigned char>(relation_id.value & 0xFFU)};
        callback(std::span<const std::byte>(payload.data(), payload.size()));
    };
}

}  // namespace

TEST_CASE("CatalogCache reuses materialized relation until invalidated")
{
    CatalogCache::instance().reset();

    std::unordered_map<std::uint64_t, std::size_t> counters;
    auto scanner = make_scanner(counters);
    const auto relation_id = kCatalogTablesRelationId;

    auto first = CatalogCache::instance().materialize(relation_id, scanner);
    REQUIRE(first);
    CHECK(counters[relation_id.value] == 1U);

    auto second = CatalogCache::instance().materialize(relation_id, scanner);
    REQUIRE(second);
    CHECK(second == first);
    CHECK(counters[relation_id.value] == 1U);

    CatalogCache::instance().invalidate(relation_id);
    auto third = CatalogCache::instance().materialize(relation_id, scanner);
    REQUIRE(third);
    CHECK(counters[relation_id.value] == 2U);
}

TEST_CASE("CatalogCache evicts least recently used relation outside retention guard")
{
    CatalogCache::instance().reset({2U, 8U});

    std::unordered_map<std::uint64_t, std::size_t> counters;
    auto scanner = make_scanner(counters);

    auto relation_a = RelationId{1U};
    auto relation_b = RelationId{2U};
    auto relation_c = RelationId{3U};

    auto cached_a = CatalogCache::instance().materialize(relation_a, scanner);
    auto cached_b = CatalogCache::instance().materialize(relation_b, scanner);
    (void)cached_a;
    (void)cached_b;
    CHECK(counters[relation_a.value] == 1U);
    CHECK(counters[relation_b.value] == 1U);

    CatalogCache::instance().invalidate(relation_b);
    auto cached_c = CatalogCache::instance().materialize(relation_c, scanner);
    (void)cached_c;

    // relation_b stays cached thanks to guard; relation_a is evicted and triggers another scan
    auto retained_b = CatalogCache::instance().materialize(relation_b, scanner);
    (void)retained_b;
    CHECK(counters[relation_b.value] == 2U);  // invalidation triggers refresh

    auto reloaded_a = CatalogCache::instance().materialize(relation_a, scanner);
    (void)reloaded_a;
    CHECK(counters[relation_a.value] == 2U);

    auto telemetry = CatalogCache::instance().telemetry();
    CHECK(telemetry.relations <= 2U);
}
