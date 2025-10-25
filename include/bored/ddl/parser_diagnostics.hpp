#pragma once

#include "bored/ddl/ddl_command.hpp"

#if BORED_ENABLE_PARSER

#include "bored/parser/grammar.hpp"

namespace bored::ddl {

DdlDiagnosticSeverity parser_severity_to_ddl(parser::ParserSeverity severity) noexcept;

DdlCommandResponse make_parser_failure_response(const parser::ParserDiagnostic& diagnostic);
DdlCommandResponse make_parser_failure_response(const std::vector<parser::ParserDiagnostic>& diagnostics);

}  // namespace bored::ddl

#endif  // BORED_ENABLE_PARSER
