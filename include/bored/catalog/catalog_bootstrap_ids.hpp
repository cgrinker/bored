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
inline constexpr RelationId kCatalogConstraintsRelationId{261U};
inline constexpr RelationId kCatalogSequencesRelationId{262U};

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
inline constexpr ColumnId kCatalogConstraintsIdColumnId{4118U};
inline constexpr ColumnId kCatalogConstraintsTableColumnId{4119U};
inline constexpr ColumnId kCatalogConstraintsTypeColumnId{4120U};
inline constexpr ColumnId kCatalogConstraintsBackingIndexColumnId{4121U};
inline constexpr ColumnId kCatalogConstraintsReferencedTableColumnId{4122U};
inline constexpr ColumnId kCatalogConstraintsKeyColumnsColumnId{4123U};
inline constexpr ColumnId kCatalogConstraintsReferencedColumnsColumnId{4124U};
inline constexpr ColumnId kCatalogConstraintsNameColumnId{4125U};
inline constexpr ColumnId kCatalogSequencesIdColumnId{4126U};
inline constexpr ColumnId kCatalogSequencesSchemaColumnId{4127U};
inline constexpr ColumnId kCatalogSequencesOwningTableColumnId{4128U};
inline constexpr ColumnId kCatalogSequencesOwningColumnColumnId{4129U};
inline constexpr ColumnId kCatalogSequencesStartValueColumnId{4130U};
inline constexpr ColumnId kCatalogSequencesIncrementColumnId{4131U};
inline constexpr ColumnId kCatalogSequencesMinValueColumnId{4132U};
inline constexpr ColumnId kCatalogSequencesMaxValueColumnId{4133U};
inline constexpr ColumnId kCatalogSequencesCacheSizeColumnId{4134U};
inline constexpr ColumnId kCatalogSequencesCycleFlagColumnId{4135U};
inline constexpr ColumnId kCatalogSequencesNameColumnId{4136U};

inline constexpr IndexId kCatalogDatabasesNameIndexId{8192U};
inline constexpr IndexId kCatalogSchemasNameIndexId{8193U};
inline constexpr IndexId kCatalogTablesNameIndexId{8194U};
inline constexpr IndexId kCatalogColumnsNameIndexId{8195U};
inline constexpr IndexId kCatalogIndexesNameIndexId{8196U};
inline constexpr IndexId kCatalogConstraintsNameIndexId{8197U};
inline constexpr IndexId kCatalogSequencesNameIndexId{8198U};

inline constexpr std::uint32_t kCatalogDatabasesPageId = 1U;
inline constexpr std::uint32_t kCatalogSchemasPageId = 2U;
inline constexpr std::uint32_t kCatalogTablesPageId = 3U;
inline constexpr std::uint32_t kCatalogColumnsPageId = 4U;
inline constexpr std::uint32_t kCatalogIndexesPageId = 5U;
inline constexpr std::uint32_t kCatalogConstraintsPageId = 6U;
inline constexpr std::uint32_t kCatalogSequencesPageId = 7U;

inline constexpr std::uint64_t kCatalogBootstrapTxnId = 1U;

}  // namespace bored::catalog
