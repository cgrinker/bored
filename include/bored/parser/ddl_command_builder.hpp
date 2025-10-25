#pragma once

#include "bored/ddl/ddl_command.hpp"
#include "bored/parser/grammar.hpp"

#include <functional>
#include <optional>
#include <string_view>
#include <vector>

namespace bored::parser {

struct DdlCommandBuilderConfig final {
    const catalog::CatalogAccessor* accessor = nullptr;
    std::optional<catalog::DatabaseId> default_database_id{};
    std::optional<catalog::SchemaId> default_schema_id{};
    std::function<std::optional<catalog::DatabaseId>(std::string_view)> database_lookup{};
    std::function<std::optional<catalog::SchemaId>(catalog::DatabaseId, std::string_view)> schema_lookup{};
};

struct DdlCommandBuilderResult final {
    std::vector<ddl::DdlCommand> commands{};
    std::vector<ParserDiagnostic> diagnostics{};
};

DdlCommandBuilderResult build_ddl_commands(const ScriptParseResult& script,
                                           const DdlCommandBuilderConfig& config);
DdlCommandBuilderResult build_ddl_commands(const ScriptStatement& statement,
                                           const DdlCommandBuilderConfig& config);

}  // namespace bored::parser
