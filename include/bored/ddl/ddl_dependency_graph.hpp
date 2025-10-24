#pragma once

#include "bored/catalog/catalog_accessor.hpp"

#include <vector>

namespace bored::ddl {

class DdlDependencyGraph final {
public:
    explicit DdlDependencyGraph(const catalog::CatalogAccessor& accessor) noexcept;

    [[nodiscard]] std::vector<catalog::CatalogIndexDescriptor> indexes_on_table(catalog::RelationId relation_id) const;

    [[nodiscard]] std::vector<catalog::CatalogIndexDescriptor> indexes_in_schema(catalog::SchemaId schema_id) const;

    [[nodiscard]] bool table_has_indexes(catalog::RelationId relation_id) const;

    [[nodiscard]] bool schema_has_indexes(catalog::SchemaId schema_id) const;

private:
    const catalog::CatalogAccessor* accessor_ = nullptr;
};

}  // namespace bored::ddl
