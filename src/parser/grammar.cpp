#include "bored/parser/grammar.hpp"

#include <tao/pegtl.hpp>

#include <cctype>
#include <unordered_set>
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

struct kw_table : keyword<'T', 'A', 'B', 'L', 'E'> {
};

struct kw_view : keyword<'V', 'I', 'E', 'W'> {
};

struct kw_as : keyword<'A', 'S'> {
};

struct kw_authorization : keyword<'A', 'U', 'T', 'H', 'O', 'R', 'I', 'Z', 'A', 'T', 'I', 'O', 'N'> {
};

struct kw_default : keyword<'D', 'E', 'F', 'A', 'U', 'L', 'T'> {
};

struct kw_primary : keyword<'P', 'R', 'I', 'M', 'A', 'R', 'Y'> {
};

struct kw_key : keyword<'K', 'E', 'Y'> {
};

struct kw_unique : keyword<'U', 'N', 'I', 'Q', 'U', 'E'> {
};

struct kw_restrict : keyword<'R', 'E', 'S', 'T', 'R', 'I', 'C', 'T'> {
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

struct schema_name_list_rule
    : pegtl::seq<schema_name_rule,
                 pegtl::star<pegtl::seq<optional_space, pegtl::one<','>, optional_space, schema_name_rule>>> {
};

struct table_name_rule : schema_name_rule {
};

struct schema_authorization_identifier : identifier_rule {
};

struct authorization_clause_rule : pegtl::seq<kw_authorization, required_space, schema_authorization_identifier> {
};

struct schema_embedded_statement_content
    : pegtl::seq<kw_create, required_space, pegtl::star<pegtl::not_one<';'>>> {
};

struct schema_embedded_statement_rule
    : pegtl::seq<pegtl::star<pegtl::space>, schema_embedded_statement_content, semicolon> {
};

struct schema_embedded_statement_start : pegtl::seq<pegtl::star<pegtl::space>, kw_create> {
};

struct schema_embedded_statements_rule
    : pegtl::star<pegtl::seq<pegtl::at<schema_embedded_statement_start>, schema_embedded_statement_rule>> {
};

struct left_paren : pegtl::one<'('> {
};

struct right_paren : pegtl::one<')'> {
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

struct column_identifier : identifier_rule {
};

struct type_identifier : identifier_rule {
};

struct not_keyword : keyword<'N', 'O', 'T'> {
};

struct null_keyword : keyword<'N', 'U', 'L', 'L'> {
};

struct constraint_not_null_rule : pegtl::seq<not_keyword, required_space, null_keyword> {
};

struct string_literal_char : pegtl::sor<pegtl::seq<pegtl::one<'\''>, pegtl::one<'\''>>, pegtl::not_one<'\''>> {
};

struct string_literal_rule
    : pegtl::seq<pegtl::one<'\''>, pegtl::star<string_literal_char>, pegtl::one<'\''>> {
};

struct numeric_literal_rule : pegtl::seq<pegtl::opt<pegtl::one<'-'>>, pegtl::plus<pegtl::digit>> {
};

struct default_identifier_rule : identifier_rule {
};

struct default_value_rule : pegtl::sor<string_literal_rule, numeric_literal_rule, default_identifier_rule> {
};

struct default_clause_rule : pegtl::seq<kw_default, required_space, default_value_rule> {
};

struct primary_key_rule : pegtl::seq<kw_primary, required_space, kw_key> {
};

struct unique_constraint_rule : kw_unique {
};

struct column_constraint_default_clause : pegtl::seq<required_space, default_clause_rule> {
};

struct column_constraint_not_null_clause : pegtl::seq<required_space, constraint_not_null_rule> {
};

struct column_constraint_primary_key_clause : pegtl::seq<required_space, primary_key_rule> {
};

struct column_constraint_unique_clause : pegtl::seq<required_space, unique_constraint_rule> {
};

struct column_constraint_clause
    : pegtl::sor<column_constraint_default_clause,
                 column_constraint_not_null_clause,
                 column_constraint_primary_key_clause,
                 column_constraint_unique_clause> {
};

struct column_definition_rule
    : pegtl::seq<column_identifier,
                 required_space,
                 type_identifier,
                 pegtl::star<column_constraint_clause>,
                 optional_space> {
};

struct column_list_rule
    : pegtl::seq<left_paren,
                 optional_space,
                 column_definition_rule,
                 pegtl::star<pegtl::seq<pegtl::one<','>, optional_space, column_definition_rule>>,
                 right_paren> {
};

struct create_table_grammar
    : pegtl::seq<optional_space,
                 kw_create,
                 required_space,
                 kw_table,
                 required_space,
                 pegtl::opt<pegtl::seq<if_not_exists_rule, required_space>>,
                 table_name_rule,
                 optional_space,
                 column_list_rule,
                 optional_space,
                 pegtl::opt<pegtl::seq<semicolon, optional_space>>,
                 pegtl::eof> {
};

struct view_definition_rule : pegtl::star<pegtl::not_one<';'>> {
};

struct create_view_grammar
    : pegtl::seq<optional_space,
                 kw_create,
                 required_space,
                 kw_view,
                 required_space,
                 pegtl::opt<pegtl::seq<if_not_exists_rule, required_space>>,
                 table_name_rule,
                 required_space,
                 kw_as,
                 required_space,
                 view_definition_rule,
                 optional_space,
                 pegtl::opt<pegtl::seq<semicolon, optional_space>>,
                 pegtl::eof> {
};

struct drop_table_grammar
    : pegtl::seq<optional_space,
                 kw_drop,
                 required_space,
                 kw_table,
                 required_space,
                 pegtl::opt<pegtl::seq<if_exists_rule, required_space>>,
                 table_name_rule,
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
                 pegtl::opt<pegtl::seq<if_not_exists_rule, pegtl::opt<required_space>>>,
                 schema_name_rule,
                 pegtl::opt<pegtl::seq<required_space, authorization_clause_rule>>,
                 schema_embedded_statements_rule,
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
                 pegtl::opt<pegtl::seq<if_exists_rule, pegtl::opt<required_space>>>,
                 schema_name_list_rule,
                 pegtl::opt<pegtl::seq<required_space, pegtl::sor<cascade_rule, kw_restrict>>>,
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

std::string trim_copy(std::string_view text)
{
    const auto first = text.find_first_not_of(" \t\r\n");
    if (first == std::string_view::npos) {
        return {};
    }
    const auto last = text.find_last_not_of(" \t\r\n");
    return std::string{text.substr(first, last - first + 1)};
}

bool iequals(std::string_view lhs, std::string_view rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }

    for (std::size_t index = 0; index < lhs.size(); ++index) {
        const auto left = static_cast<unsigned char>(lhs[index]);
        const auto right = static_cast<unsigned char>(rhs[index]);
        if (std::tolower(left) != std::tolower(right)) {
            return false;
        }
    }

    return true;
}

std::string_view next_token(std::string_view text, std::size_t& offset)
{
    while (offset < text.size() && std::isspace(static_cast<unsigned char>(text[offset])) != 0) {
        ++offset;
    }

    const auto start = offset;
    while (offset < text.size() && std::isspace(static_cast<unsigned char>(text[offset])) == 0) {
        ++offset;
    }

    return text.substr(start, offset - start);
}

std::string format_parse_message(std::string_view message)
{
    constexpr std::string_view expected_prefix = "expected ";
    if (message.rfind(expected_prefix, 0) == 0U && message.size() > expected_prefix.size()) {
        auto detail = message.substr(expected_prefix.size());
        if (!detail.empty() && detail.front() == '\'' && detail.back() == '\'' && detail.size() > 2) {
            detail = detail.substr(1, detail.size() - 2);
        }
        return "Missing " + std::string{detail};
    }
    return std::string{message};
}

std::string_view extract_token(std::string_view input, std::size_t offset)
{
    if (input.empty()) {
        return {};
    }

    offset = std::min(offset, input.size() - 1U);

    auto is_separator = [](char ch) {
        const auto unsigned_ch = static_cast<unsigned char>(ch);
        return std::isspace(unsigned_ch) != 0 || ch == ';' || ch == ',' || ch == '(' || ch == ')';
    };

    std::size_t begin = offset;
    while (begin > 0U && !is_separator(input[begin - 1U])) {
        --begin;
    }

    std::size_t end = offset;
    while (end < input.size() && !is_separator(input[end])) {
        ++end;
    }

    return input.substr(begin, end - begin);
}

ParserDiagnostic make_parse_error(const pegtl::parse_error& error, std::string_view source)
{
    ParserDiagnostic diagnostic{};
    diagnostic.severity = ParserSeverity::Error;
    diagnostic.message = format_parse_message(error.message());

    if (!error.positions().empty()) {
        const auto& position = error.positions().front();
        diagnostic.line = static_cast<std::size_t>(position.line);
        diagnostic.column = static_cast<std::size_t>(position.column);

        const auto byte_index = static_cast<std::size_t>(position.byte);
        if (!source.empty() && byte_index < source.size()) {
            const auto token = trim_copy(extract_token(source, byte_index));
            if (!token.empty()) {
                diagnostic.message += " near '" + token + "'";
            }
        } else if (byte_index >= source.size()) {
            diagnostic.message += " at end of input";
        }
    }

    return diagnostic;
}

void append_duplicate_column_diagnostics(const CreateTableStatement& statement,
                                         std::vector<ParserDiagnostic>& diagnostics)
{
    std::unordered_set<std::string> seen{};
    for (const auto& column : statement.columns) {
        auto [_, inserted] = seen.insert(column.name.value);
        if (!inserted) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Duplicate column name '" + column.name.value + "'";
            diagnostics.push_back(std::move(diagnostic));
        }
    }
}

void append_duplicate_schema_diagnostics(const DropSchemaStatement& statement,
                                         std::vector<ParserDiagnostic>& diagnostics)
{
    std::unordered_set<std::string> seen{};
    for (const auto& schema : statement.schemas) {
        std::string key = schema.database.value.empty() ? schema.name.value
                                                        : schema.database.value + "." + schema.name.value;
        auto [_, inserted] = seen.insert(key);
        if (!inserted) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Duplicate schema '" + key + "' in DROP SCHEMA list";
            diagnostics.push_back(std::move(diagnostic));
        }
    }
}

void convert_embedded_statements(const std::vector<std::string>& raw_statements,
                                 CreateSchemaStatement& statement,
                                 std::vector<ParserDiagnostic>& diagnostics)
{
    for (const auto& raw : raw_statements) {
        const auto trimmed = trim_copy(raw);
        if (trimmed.empty()) {
            continue;
        }

        std::size_t offset = 0U;
        const auto first = next_token(trimmed, offset);
        const auto second = next_token(trimmed, offset);

        if (iequals(first, "CREATE") && iequals(second, "TABLE")) {
            auto table_result = parse_create_table(trimmed);
            diagnostics.insert(diagnostics.end(),
                               table_result.diagnostics.begin(),
                               table_result.diagnostics.end());
            if (table_result.ast) {
                statement.embedded_statements.emplace_back(std::move(*table_result.ast));
            } else if (table_result.diagnostics.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Failed to parse embedded CREATE TABLE statement";
                diagnostics.push_back(std::move(diagnostic));
            }
            continue;
        }

        if (iequals(first, "CREATE") && iequals(second, "VIEW")) {
            auto view_result = parse_create_view(trimmed);
            diagnostics.insert(diagnostics.end(),
                               view_result.diagnostics.begin(),
                               view_result.diagnostics.end());
            if (view_result.ast) {
                statement.embedded_statements.emplace_back(std::move(*view_result.ast));
            } else if (view_result.diagnostics.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Failed to parse embedded CREATE VIEW statement";
                diagnostics.push_back(std::move(diagnostic));
            }
            continue;
        }

        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Unsupported embedded CREATE statement '" + trimmed + "'";
        diagnostics.push_back(std::move(diagnostic));
    }
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
    static void apply(const Input&, CreateSchemaStatement&, std::vector<std::string>&)
    {
    }
};

template <>
struct create_schema_action<if_not_exists_rule> {
    template <typename Input>
    static void apply(const Input&, CreateSchemaStatement& statement, std::vector<std::string>&)
    {
        statement.if_not_exists = true;
    }
};

template <>
struct create_schema_action<schema_name_head> {
    template <typename Input>
    static void apply(const Input& in, CreateSchemaStatement& statement, std::vector<std::string>&)
    {
        statement.database.value.clear();
        statement.name.value = in.string();
    }
};

template <>
struct create_schema_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, CreateSchemaStatement& statement, std::vector<std::string>&)
    {
        statement.database.value = statement.name.value;
        statement.name.value = in.string();
    }
};

template <>
struct create_schema_action<schema_authorization_identifier> {
    template <typename Input>
    static void apply(const Input& in, CreateSchemaStatement& statement, std::vector<std::string>&)
    {
        statement.authorization = Identifier{};
        statement.authorization->value = in.string();
    }
};

template <>
struct create_schema_action<schema_embedded_statement_content> {
    template <typename Input>
    static void apply(const Input& in,
                      CreateSchemaStatement&,
                      std::vector<std::string>& statements)
    {
        auto text = trim_copy(in.string());
        if (!text.empty()) {
            statements.push_back(std::move(text));
        }
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
        auto& schema = statement.schemas.emplace_back();
        schema.database.value.clear();
        schema.name.value = in.string();
    }
};

template <>
struct drop_schema_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, DropSchemaStatement& statement)
    {
        if (!statement.schemas.empty()) {
            auto& schema = statement.schemas.back();
            schema.database.value = schema.name.value;
            schema.name.value = in.string();
        }
    }
};

template <>
struct drop_schema_action<cascade_rule> {
    template <typename Input>
    static void apply(const Input&, DropSchemaStatement& statement)
    {
        statement.behavior = DropSchemaStatement::Behavior::Cascade;
    }
};

template <>
struct drop_schema_action<kw_restrict> {
    template <typename Input>
    static void apply(const Input&, DropSchemaStatement& statement)
    {
        statement.behavior = DropSchemaStatement::Behavior::Restrict;
    }
};

template <typename Rule>
struct create_table_action {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement&)
    {
    }
};

template <>
struct create_table_action<if_not_exists_rule> {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement& statement)
    {
        statement.if_not_exists = true;
    }
};

template <>
struct create_table_action<schema_name_head> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement)
    {
        statement.schema.value.clear();
        statement.name.value = in.string();
    }
};

template <>
struct create_table_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement)
    {
        statement.schema.value = statement.name.value;
        statement.name.value = in.string();
    }
};

template <>
struct create_table_action<column_identifier> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement)
    {
        auto& column = statement.columns.emplace_back();
        column.name.value = in.string();
        column.not_null = false;
        column.primary_key = false;
        column.unique = false;
        column.default_expression.reset();
    }
};

template <>
struct create_table_action<type_identifier> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement)
    {
        if (!statement.columns.empty()) {
            statement.columns.back().type_name.value = in.string();
        }
    }
};

template <>
struct create_table_action<constraint_not_null_rule> {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement& statement)
    {
        if (!statement.columns.empty()) {
            statement.columns.back().not_null = true;
        }
    }
};

template <>
struct create_table_action<default_value_rule> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement)
    {
        if (!statement.columns.empty()) {
            statement.columns.back().default_expression = in.string();
        }
    }
};

template <>
struct create_table_action<primary_key_rule> {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement& statement)
    {
        if (!statement.columns.empty()) {
            auto& column = statement.columns.back();
            column.primary_key = true;
            column.not_null = true;
        }
    }
};

template <>
struct create_table_action<unique_constraint_rule> {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement& statement)
    {
        if (!statement.columns.empty()) {
            statement.columns.back().unique = true;
        }
    }
};

template <typename Rule>
struct create_view_action {
    template <typename Input>
    static void apply(const Input&, CreateViewStatement&)
    {
    }
};

template <>
struct create_view_action<if_not_exists_rule> {
    template <typename Input>
    static void apply(const Input&, CreateViewStatement& statement)
    {
        statement.if_not_exists = true;
    }
};

template <>
struct create_view_action<schema_name_head> {
    template <typename Input>
    static void apply(const Input& in, CreateViewStatement& statement)
    {
        statement.schema.value.clear();
        statement.name.value = in.string();
    }
};

template <>
struct create_view_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, CreateViewStatement& statement)
    {
        statement.schema.value = statement.name.value;
        statement.name.value = in.string();
    }
};

template <>
struct create_view_action<view_definition_rule> {
    template <typename Input>
    static void apply(const Input& in, CreateViewStatement& statement)
    {
        statement.definition = trim_copy(in.string());
    }
};

template <typename Rule>
struct drop_table_action {
    template <typename Input>
    static void apply(const Input&, DropTableStatement&)
    {
    }
};

template <>
struct drop_table_action<if_exists_rule> {
    template <typename Input>
    static void apply(const Input&, DropTableStatement& statement)
    {
        statement.if_exists = true;
    }
};

template <>
struct drop_table_action<schema_name_head> {
    template <typename Input>
    static void apply(const Input& in, DropTableStatement& statement)
    {
        statement.schema.value.clear();
        statement.name.value = in.string();
    }
};

template <>
struct drop_table_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, DropTableStatement& statement)
    {
        statement.schema.value = statement.name.value;
        statement.name.value = in.string();
    }
};

template <>
struct drop_table_action<cascade_rule> {
    template <typename Input>
    static void apply(const Input&, DropTableStatement& statement)
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
        result.diagnostics.push_back(make_parse_error(error, input));
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
        result.diagnostics.push_back(make_parse_error(error, input));
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
        result.diagnostics.push_back(make_parse_error(error, input));
    }

    return result;
}

ParseResult<CreateSchemaStatement> parse_create_schema(std::string_view input)
{
    ParseResult<CreateSchemaStatement> result{};
    pegtl::memory_input in(input, "create_schema");
    CreateSchemaStatement statement{};
    std::vector<std::string> embedded_sql{};

    try {
        const auto parsed = pegtl::parse<create_schema_grammar, create_schema_action>(in, statement, embedded_sql);
        if (parsed) {
            convert_embedded_statements(embedded_sql, statement, result.diagnostics);
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
        result.diagnostics.push_back(make_parse_error(error, input));
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
            append_duplicate_schema_diagnostics(statement, result.diagnostics);
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
        result.diagnostics.push_back(make_parse_error(error, input));
    }

    return result;
}

ParseResult<CreateTableStatement> parse_create_table(std::string_view input)
{
    ParseResult<CreateTableStatement> result{};
    pegtl::memory_input in(input, "create_table");
    CreateTableStatement statement{};

    try {
        const auto parsed = pegtl::parse<create_table_grammar, create_table_action>(in, statement);
        if (parsed) {
            append_duplicate_column_diagnostics(statement, result.diagnostics);
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "input did not match CREATE TABLE grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error, input));
    }

    return result;
}

ParseResult<CreateViewStatement> parse_create_view(std::string_view input)
{
    ParseResult<CreateViewStatement> result{};
    pegtl::memory_input in(input, "create_view");
    CreateViewStatement statement{};

    try {
        const auto parsed = pegtl::parse<create_view_grammar, create_view_action>(in, statement);
        if (parsed) {
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "input did not match CREATE VIEW grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error, input));
    }

    return result;
}

ParseResult<DropTableStatement> parse_drop_table(std::string_view input)
{
    ParseResult<DropTableStatement> result{};
    pegtl::memory_input in(input, "drop_table");
    DropTableStatement statement{};

    try {
        const auto parsed = pegtl::parse<drop_table_grammar, drop_table_action>(in, statement);
        if (parsed) {
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "input did not match DROP TABLE grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error, input));
    }

    return result;
}

}  // namespace bored::parser
