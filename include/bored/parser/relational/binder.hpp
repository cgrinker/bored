#pragma once

#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/ast.hpp"

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bored::parser::relational {

struct ColumnMetadata final {
    catalog::ColumnId column_id{};
    catalog::CatalogColumnType column_type = catalog::CatalogColumnType::Unknown;
    std::string name{};
};

struct TableMetadata final {
    catalog::DatabaseId database_id{};
    catalog::SchemaId schema_id{};
    catalog::RelationId relation_id{};
    std::optional<std::string> database_name{};
    std::string schema_name{};
    std::string table_name{};
    std::vector<ColumnMetadata> columns{};
};

class BinderCatalog {
public:
    virtual ~BinderCatalog() = default;
    virtual std::optional<TableMetadata> lookup_table(std::optional<std::string_view> schema,
                                                      std::string_view table) const = 0;
};

struct BinderConfig final {
    const BinderCatalog* catalog = nullptr;
    std::optional<std::string> default_schema{};
};

struct BindingResult final {
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return diagnostics.empty(); }
};

BindingResult bind_select(const BinderConfig& config, SelectStatement& statement);
BindingResult bind_insert(const BinderConfig& config, InsertStatement& statement);
BindingResult bind_update(const BinderConfig& config, UpdateStatement& statement);
BindingResult bind_delete(const BinderConfig& config, DeleteStatement& statement);

}  // namespace bored::parser::relational
