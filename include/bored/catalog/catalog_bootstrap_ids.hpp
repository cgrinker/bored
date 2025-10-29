#pragma once

#include "bored/catalog/catalog_ids.hpp"

#include <cstdint>

namespace bored::catalog {

inline constexpr DatabaseId kSystemDatabaseId{1U};
inline constexpr SchemaId kSystemSchemaId{2U};

inline constexpr RelationId kCatalogDatabasesRelationId{256U};
inline constexpr RelationId kCatalogSchemasRelationId{257U};
inline constexpr RelationId kCatalogTablesRelationId{258U};
inline constexpr RelationId kCatalogColumnsRelationId{259U};
inline constexpr RelationId kCatalogIndexesRelationId{260U};

inline constexpr ColumnId kCatalogDatabasesIdColumnId{4096U};
inline constexpr ColumnId kCatalogDatabasesNameColumnId{4097U};

inline constexpr ColumnId kCatalogSchemasIdColumnId{4098U};
inline constexpr ColumnId kCatalogSchemasDatabaseColumnId{4099U};
inline constexpr ColumnId kCatalogSchemasNameColumnId{4100U};

inline constexpr ColumnId kCatalogTablesIdColumnId{4101U};
inline constexpr ColumnId kCatalogTablesSchemaColumnId{4102U};
inline constexpr ColumnId kCatalogTablesTypeColumnId{4103U};
inline constexpr ColumnId kCatalogTablesPageColumnId{4104U};
inline constexpr ColumnId kCatalogTablesNameColumnId{4105U};

inline constexpr ColumnId kCatalogColumnsIdColumnId{4106U};
inline constexpr ColumnId kCatalogColumnsTableColumnId{4107U};
inline constexpr ColumnId kCatalogColumnsTypeColumnId{4108U};
inline constexpr ColumnId kCatalogColumnsOrdinalColumnId{4109U};
inline constexpr ColumnId kCatalogColumnsNameColumnId{4110U};

inline constexpr ColumnId kCatalogIndexesIdColumnId{4111U};
inline constexpr ColumnId kCatalogIndexesTableColumnId{4112U};
inline constexpr ColumnId kCatalogIndexesTypeColumnId{4113U};
inline constexpr ColumnId kCatalogIndexesRootPageColumnId{4114U};
inline constexpr ColumnId kCatalogIndexesComparatorColumnId{4115U};
inline constexpr ColumnId kCatalogIndexesFanoutColumnId{4116U};
inline constexpr ColumnId kCatalogIndexesNameColumnId{4117U};

inline constexpr IndexId kCatalogDatabasesNameIndexId{8192U};
inline constexpr IndexId kCatalogSchemasNameIndexId{8193U};
inline constexpr IndexId kCatalogTablesNameIndexId{8194U};
inline constexpr IndexId kCatalogColumnsNameIndexId{8195U};
inline constexpr IndexId kCatalogIndexesNameIndexId{8196U};

inline constexpr std::uint32_t kCatalogDatabasesPageId = 1U;
inline constexpr std::uint32_t kCatalogSchemasPageId = 2U;
inline constexpr std::uint32_t kCatalogTablesPageId = 3U;
inline constexpr std::uint32_t kCatalogColumnsPageId = 4U;
inline constexpr std::uint32_t kCatalogIndexesPageId = 5U;

inline constexpr std::uint64_t kCatalogBootstrapTxnId = 1U;

}  // namespace bored::catalog
