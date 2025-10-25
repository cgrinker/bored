#include "bored/ddl/parser_diagnostics.hpp"

#if BORED_ENABLE_PARSER

#include <catch2/catch_test_macros.hpp>

using bored::ddl::DdlDiagnosticSeverity;
using bored::ddl::DdlErrc;
using bored::ddl::make_parser_failure_response;
using bored::ddl::parser_severity_to_ddl;
using bored::parser::ParserDiagnostic;
using bored::parser::ParserSeverity;

TEST_CASE("parser_severity_to_ddl maps severities")
{
    CHECK(parser_severity_to_ddl(ParserSeverity::Info) == DdlDiagnosticSeverity::Info);
    CHECK(parser_severity_to_ddl(ParserSeverity::Warning) == DdlDiagnosticSeverity::Warning);
    CHECK(parser_severity_to_ddl(ParserSeverity::Error) == DdlDiagnosticSeverity::Error);
}

TEST_CASE("make_parser_failure_response maps warning diagnostics to validation failures")
{
    ParserDiagnostic diagnostic{};
    diagnostic.severity = ParserSeverity::Warning;
    diagnostic.message = "Duplicate column name 'id'";
    diagnostic.statement = "CREATE TABLE accounts (id INT, id INT);";
    diagnostic.remediation_hints = {"Remove or rename the duplicate column before retrying the statement."};

    const auto response = make_parser_failure_response(diagnostic);

    CHECK_FALSE(response.success);
    CHECK(response.error == make_error_code(DdlErrc::ValidationFailed));
    CHECK(response.severity == DdlDiagnosticSeverity::Warning);
    CHECK(response.message == diagnostic.message);
    REQUIRE(response.remediation_hints.size() == 2U);
    CHECK(response.remediation_hints[0] == diagnostic.remediation_hints[0]);
    CHECK(response.remediation_hints[1] == "Statement context: " + diagnostic.statement);
}

TEST_CASE("make_parser_failure_response escalates parser errors to execution failures")
{
    ParserDiagnostic diagnostic{};
    diagnostic.severity = ParserSeverity::Error;
    diagnostic.message = "Failed to parse embedded CREATE TABLE statement";
    diagnostic.statement = "CREATE TABLE broken (...)";

    const auto response = make_parser_failure_response(diagnostic);

    CHECK_FALSE(response.success);
    CHECK(response.error == make_error_code(DdlErrc::ExecutionFailed));
    CHECK(response.severity == DdlDiagnosticSeverity::Error);
    CHECK(response.message == diagnostic.message);
    REQUIRE(response.remediation_hints.size() == 2U);
    CHECK(response.remediation_hints[0] == "Statement context: " + diagnostic.statement);
    CHECK(response.remediation_hints[1] == "Capture the failing statement and report a parser bug.");
}

#endif  // BORED_ENABLE_PARSER
