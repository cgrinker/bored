#include "bored/ddl/ddl_dependency_graph.hpp"

#include <utility>

namespace bored::ddl {

DdlDependencyGraph::DdlDependencyGraph(const catalog::CatalogAccessor& accessor) noexcept
    : accessor_{&accessor}
{
}

std::vector<catalog::CatalogIndexDescriptor> DdlDependencyGraph::indexes_on_table(catalog::RelationId relation_id) const
{
    if (!accessor_ || !relation_id.is_valid()) {
        return {};
    }
    return accessor_->indexes(relation_id);
}

std::vector<catalog::CatalogIndexDescriptor> DdlDependencyGraph::indexes_in_schema(catalog::SchemaId schema_id) const
{
    if (!accessor_ || !schema_id.is_valid()) {
        return {};
    }
    return accessor_->indexes_for_schema(schema_id);
}

bool DdlDependencyGraph::table_has_indexes(catalog::RelationId relation_id) const
{
    auto dependents = indexes_on_table(relation_id);
    return !dependents.empty();
}

bool DdlDependencyGraph::schema_has_indexes(catalog::SchemaId schema_id) const
{
    auto dependents = indexes_in_schema(schema_id);
    return !dependents.empty();
}

}  // namespace bored::ddl
