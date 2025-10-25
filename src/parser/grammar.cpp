#include "bored/parser/grammar.hpp"

#include <tao/pegtl.hpp>

#include <utility>

namespace bored::parser {
namespace {

namespace pegtl = tao::pegtl;

struct identifier_head : pegtl::sor<pegtl::alpha, pegtl::one<'_'>> {
};

struct identifier_tail : pegtl::sor<identifier_head, pegtl::digit> {
};

struct identifier_rule : pegtl::seq<identifier_head, pegtl::star<identifier_tail>> {
};

struct identifier_grammar : pegtl::must<pegtl::pad<identifier_rule, pegtl::space>, pegtl::eof> {
};

struct optional_space : pegtl::star<pegtl::space> {
};

struct required_space : pegtl::plus<pegtl::space> {
};

struct semicolon : pegtl::one<';'> {
};

struct dot : pegtl::one<'.'> {
};

template <char... Cs>
struct keyword : pegtl::seq<pegtl::istring<Cs...>, pegtl::not_at<identifier_tail>> {
};

struct kw_create : keyword<'C', 'R', 'E', 'A', 'T', 'E'> {
};

struct kw_drop : keyword<'D', 'R', 'O', 'P'> {
};

struct kw_database : keyword<'D', 'A', 'T', 'A', 'B', 'A', 'S', 'E'> {
};

struct kw_schema : keyword<'S', 'C', 'H', 'E', 'M', 'A'> {
};

struct kw_if : keyword<'I', 'F'> {
};

struct kw_not : keyword<'N', 'O', 'T'> {
};

struct kw_exists : keyword<'E', 'X', 'I', 'S', 'T', 'S'> {
};

struct kw_cascade : keyword<'C', 'A', 'S', 'C', 'A', 'D', 'E'> {
};

struct if_not_exists_rule : pegtl::seq<kw_if, required_space, kw_not, required_space, kw_exists> {
};

struct if_exists_rule : pegtl::seq<kw_if, required_space, kw_exists> {
};

struct cascade_rule : kw_cascade {
};

struct database_identifier : identifier_rule {
};

struct schema_name_head : identifier_rule {
};

struct schema_name_tail : identifier_rule {
};

struct schema_name_rule
    : pegtl::seq<schema_name_head,
                 pegtl::opt<pegtl::seq<optional_space, dot, optional_space, schema_name_tail>>> {
};

struct create_database_grammar
    : pegtl::seq<optional_space,
                 kw_create,
                 required_space,
                 kw_database,
                 required_space,
                 pegtl::opt<pegtl::seq<if_not_exists_rule, required_space>>,
                 database_identifier,
                 optional_space,
                 pegtl::opt<pegtl::seq<semicolon, optional_space>>,
                 pegtl::eof> {
};

struct drop_database_grammar
    : pegtl::seq<optional_space,
                 kw_drop,
                 required_space,
                 kw_database,
                 required_space,
                 pegtl::opt<pegtl::seq<if_exists_rule, required_space>>,
                 database_identifier,
                 pegtl::opt<pegtl::seq<required_space, cascade_rule>>,
                 optional_space,
                 pegtl::opt<pegtl::seq<semicolon, optional_space>>,
                 pegtl::eof> {
};

struct create_schema_grammar
    : pegtl::seq<optional_space,
                 kw_create,
                 required_space,
                 kw_schema,
                 required_space,
                 pegtl::opt<pegtl::seq<if_not_exists_rule, required_space>>,
                 schema_name_rule,
                 optional_space,
                 pegtl::opt<pegtl::seq<semicolon, optional_space>>,
                 pegtl::eof> {
};

struct drop_schema_grammar
    : pegtl::seq<optional_space,
                 kw_drop,
                 required_space,
                 kw_schema,
                 required_space,
                 pegtl::opt<pegtl::seq<if_exists_rule, required_space>>,
                 schema_name_rule,
                 pegtl::opt<pegtl::seq<required_space, cascade_rule>>,
                 optional_space,
                 pegtl::opt<pegtl::seq<semicolon, optional_space>>,
                 pegtl::eof> {
};

template <typename Rule>
struct identifier_action {
    template <typename Input>
    static void apply(const Input&, Identifier&)
    {
        // No-op by default
    }
};

template <>
struct identifier_action<identifier_rule> {
    template <typename Input>
    static void apply(const Input& in, Identifier& identifier)
    {
        identifier.value = in.string();
    }
};

ParserDiagnostic make_parse_error(const pegtl::parse_error& error)
{
    ParserDiagnostic diagnostic{};
    diagnostic.severity = ParserSeverity::Error;
    diagnostic.message = error.message();
    if (!error.positions().empty()) {
        const auto& position = error.positions().front();
        diagnostic.line = static_cast<std::size_t>(position.line);
        diagnostic.column = static_cast<std::size_t>(position.column);
    }
    return diagnostic;
}

template <typename Rule>
struct create_database_action {
    template <typename Input>
    static void apply(const Input&, CreateDatabaseStatement&)
    {
    }
};

template <>
struct create_database_action<if_not_exists_rule> {
    template <typename Input>
    static void apply(const Input&, CreateDatabaseStatement& statement)
    {
        statement.if_not_exists = true;
    }
};

template <>
struct create_database_action<database_identifier> {
    template <typename Input>
    static void apply(const Input& in, CreateDatabaseStatement& statement)
    {
        statement.name.value = in.string();
    }
};

template <typename Rule>
struct drop_database_action {
    template <typename Input>
    static void apply(const Input&, DropDatabaseStatement&)
    {
    }
};

template <>
struct drop_database_action<if_exists_rule> {
    template <typename Input>
    static void apply(const Input&, DropDatabaseStatement& statement)
    {
        statement.if_exists = true;
    }
};

template <>
struct drop_database_action<database_identifier> {
    template <typename Input>
    static void apply(const Input& in, DropDatabaseStatement& statement)
    {
        statement.name.value = in.string();
    }
};

template <>
struct drop_database_action<cascade_rule> {
    template <typename Input>
    static void apply(const Input&, DropDatabaseStatement& statement)
    {
        statement.cascade = true;
    }
};

template <typename Rule>
struct create_schema_action {
    template <typename Input>
    static void apply(const Input&, CreateSchemaStatement&)
    {
    }
};

template <>
struct create_schema_action<if_not_exists_rule> {
    template <typename Input>
    static void apply(const Input&, CreateSchemaStatement& statement)
    {
        statement.if_not_exists = true;
    }
};

template <>
struct create_schema_action<schema_name_head> {
    template <typename Input>
    static void apply(const Input& in, CreateSchemaStatement& statement)
    {
        statement.database.value.clear();
        statement.name.value = in.string();
    }
};

template <>
struct create_schema_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, CreateSchemaStatement& statement)
    {
        statement.database.value = statement.name.value;
        statement.name.value = in.string();
    }
};

template <typename Rule>
struct drop_schema_action {
    template <typename Input>
    static void apply(const Input&, DropSchemaStatement&)
    {
    }
};

template <>
struct drop_schema_action<if_exists_rule> {
    template <typename Input>
    static void apply(const Input&, DropSchemaStatement& statement)
    {
        statement.if_exists = true;
    }
};

template <>
struct drop_schema_action<schema_name_head> {
    template <typename Input>
    static void apply(const Input& in, DropSchemaStatement& statement)
    {
        statement.database.value.clear();
        statement.name.value = in.string();
    }
};

template <>
struct drop_schema_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, DropSchemaStatement& statement)
    {
        statement.database.value = statement.name.value;
        statement.name.value = in.string();
    }
};

template <>
struct drop_schema_action<cascade_rule> {
    template <typename Input>
    static void apply(const Input&, DropSchemaStatement& statement)
    {
        statement.cascade = true;
    }
};

}  // namespace

ParseResult<Identifier> parse_identifier(std::string_view input)
{
    ParseResult<Identifier> result{};
    pegtl::memory_input in(input, "identifier");
    Identifier identifier{};

    try {
        const auto parsed = pegtl::parse<identifier_grammar, identifier_action>(in, identifier);
        if (parsed) {
            result.ast = std::move(identifier);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "input did not match identifier grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error));
    }

    return result;
}

ParseResult<CreateDatabaseStatement> parse_create_database(std::string_view input)
{
    ParseResult<CreateDatabaseStatement> result{};
    pegtl::memory_input in(input, "create_database");
    CreateDatabaseStatement statement{};

    try {
        const auto parsed = pegtl::parse<create_database_grammar, create_database_action>(in, statement);
        if (parsed) {
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "input did not match CREATE DATABASE grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error));
    }

    return result;
}

ParseResult<DropDatabaseStatement> parse_drop_database(std::string_view input)
{
    ParseResult<DropDatabaseStatement> result{};
    pegtl::memory_input in(input, "drop_database");
    DropDatabaseStatement statement{};

    try {
        const auto parsed = pegtl::parse<drop_database_grammar, drop_database_action>(in, statement);
        if (parsed) {
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "input did not match DROP DATABASE grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error));
    }

    return result;
}

ParseResult<CreateSchemaStatement> parse_create_schema(std::string_view input)
{
    ParseResult<CreateSchemaStatement> result{};
    pegtl::memory_input in(input, "create_schema");
    CreateSchemaStatement statement{};

    try {
        const auto parsed = pegtl::parse<create_schema_grammar, create_schema_action>(in, statement);
        if (parsed) {
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "input did not match CREATE SCHEMA grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error));
    }

    return result;
}

ParseResult<DropSchemaStatement> parse_drop_schema(std::string_view input)
{
    ParseResult<DropSchemaStatement> result{};
    pegtl::memory_input in(input, "drop_schema");
    DropSchemaStatement statement{};

    try {
        const auto parsed = pegtl::parse<drop_schema_grammar, drop_schema_action>(in, statement);
        if (parsed) {
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "input did not match DROP SCHEMA grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error));
    }

    return result;
}

}  // namespace bored::parser
