#pragma once

#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_relations.hpp"

#include <optional>
#include <cstdint>

namespace bored::catalog {

inline std::optional<std::uint32_t> catalog_relation_page(RelationId relation_id) noexcept
{
    if (relation_id == kCatalogDatabasesRelationId) {
        return kCatalogDatabasesPageId;
    }
    if (relation_id == kCatalogSchemasRelationId) {
        return kCatalogSchemasPageId;
    }
    if (relation_id == kCatalogTablesRelationId) {
        return kCatalogTablesPageId;
    }
    if (relation_id == kCatalogColumnsRelationId) {
        return kCatalogColumnsPageId;
    }
    if (relation_id == kCatalogIndexesRelationId) {
        return kCatalogIndexesPageId;
    }
    return std::nullopt;
}

}  // namespace bored::catalog
