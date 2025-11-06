#pragma once

#include "bored/catalog/catalog_relations.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace bored::catalog {

class CatalogAccessor;

inline constexpr std::uint32_t kCatalogIntrospectionSchemaVersion = 1U;

enum class CatalogRelationKind : std::uint8_t {
    Table = 0,
    SystemTable = 1,
    View = 2
};

struct CatalogRelationSummary final {
    std::string database_name;
    std::string schema_name;
    std::string relation_name;
    CatalogRelationKind relation_kind = CatalogRelationKind::Table;
    std::uint32_t root_page_id = 0U;
    std::uint32_t column_count = 0U;
};

struct CatalogIndexSummary final {
    std::string schema_name;
    std::string relation_name;
    std::string index_name;
    CatalogIndexType index_type = CatalogIndexType::Unknown;
    std::string comparator;
    std::uint16_t max_fanout = 0U;
    std::uint32_t root_page_id = 0U;
};

struct CatalogViewSummary final {
    std::string database_name;
    std::string schema_name;
    std::string view_name;
    std::string definition;
};

struct CatalogIntrospectionSnapshot final {
    std::uint32_t schema_version = kCatalogIntrospectionSchemaVersion;
    std::vector<CatalogRelationSummary> relations;
    std::vector<CatalogIndexSummary> indexes;
    std::vector<CatalogViewSummary> views;
};

CatalogIntrospectionSnapshot collect_catalog_introspection(const CatalogAccessor& accessor);

std::string catalog_introspection_to_json(const CatalogIntrospectionSnapshot& snapshot);

using CatalogIntrospectionSampler = std::function<CatalogIntrospectionSnapshot()>;

void set_global_catalog_introspection_sampler(CatalogIntrospectionSampler sampler) noexcept;
CatalogIntrospectionSampler get_global_catalog_introspection_sampler() noexcept;
CatalogIntrospectionSnapshot collect_global_catalog_introspection();

}  // namespace bored::catalog
