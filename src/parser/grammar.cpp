#include "bored/parser/grammar.hpp"
#include "bored/parser/expression_primitives.hpp"

#include <tao/pegtl.hpp>

#include <cctype>
#include <optional>
#include <regex>
namespace {

relational::IdentifierExpression& make_identifier_expression(relational::AstArena& arena, std::string_view text)
{
    auto& expression = arena.make<relational::IdentifierExpression>();
    expression.name = make_qualified_name(text);
    return expression;
}

relational::LiteralExpression& make_numeric_literal_expression(relational::AstArena& arena, std::string token)
{
    auto& expression = arena.make<relational::LiteralExpression>();
    if (is_decimal_literal(token)) {
        expression.tag = relational::LiteralTag::Decimal;
    } else {
        expression.tag = relational::LiteralTag::Integer;
    }
    expression.text = std::move(token);
    return expression;
}

relational::LiteralExpression& make_string_literal_expression(relational::AstArena& arena, std::string text)
{
    auto& expression = arena.make<relational::LiteralExpression>();
    expression.tag = relational::LiteralTag::String;
    expression.text = std::move(text);
    return expression;
}

relational::LiteralExpression& make_boolean_literal_expression(relational::AstArena& arena, bool value)
{
    auto& expression = arena.make<relational::LiteralExpression>();
    expression.tag = relational::LiteralTag::Boolean;
    expression.boolean_value = value;
    expression.text = value ? "TRUE" : "FALSE";
    return expression;
}

relational::LiteralExpression& make_null_literal_expression(relational::AstArena& arena)
{
    auto& expression = arena.make<relational::LiteralExpression>();
    expression.tag = relational::LiteralTag::Null;
    expression.text.clear();
    return expression;
}

relational::BinaryExpression& make_binary_expression(relational::AstArena& arena,
                                                     relational::BinaryOperator op,
                                                     relational::Expression& left,
                                                     relational::Expression& right)
{
    auto& expression = arena.make<relational::BinaryExpression>();
    expression.op = op;
    expression.left = &left;
    expression.right = &right;
    return expression;
}

}  // namespace

UpdateParseResult parse_update(std::string_view input)
{
    UpdateParseResult result{};
    const auto normalized = strip_leading_comments(input);
    const auto statement_text = strip_trailing_semicolon(normalized);
    if (statement_text.empty()) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement is empty";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Provide an UPDATE <table> SET ... WHERE ... statement."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    if (!starts_with_ci(statement_text, "UPDATE")) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Only UPDATE statements are supported";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Begin the command with the UPDATE keyword."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    constexpr std::size_t kUpdateLength = 6U;
    if (statement_text.size() <= kUpdateLength) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement requires a target table";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Specify a table name immediately after UPDATE."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    auto rest = trim_copy(statement_text.substr(kUpdateLength));
    if (rest.empty()) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement requires a target table";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Specify a table name immediately after UPDATE."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    std::size_t position = 0U;
    auto rest_view = std::string_view(rest);
    skip_whitespace(rest_view, position);
    const auto table_start = position;
    while (position < rest_view.size() && !std::isspace(static_cast<unsigned char>(rest_view[position]))) {
        ++position;
    }
    if (table_start == position) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement requires a target table";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Specify a table name immediately after UPDATE."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }
    const auto table_spec = trim_copy(rest_view.substr(table_start, position - table_start));
    skip_whitespace(rest_view, position);

    if (position + 3U > rest_view.size() || uppercase_copy(rest_view.substr(position, 3U)) != "SET") {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement must include a SET clause";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Add SET <column> = <expression> before the WHERE clause."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }
    position += 3U;
    skip_whitespace(rest_view, position);

    const auto where_offset = find_keyword_ci(rest_view.substr(position), "WHERE");
    if (where_offset == std::string::npos) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement requires a WHERE clause";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Add WHERE <column> = <value> to limit the rows being updated."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    auto assignment_text = trim_copy(rest_view.substr(position, where_offset));
    position += where_offset + 5U;
    skip_whitespace(rest_view, position);
    auto where_text = trim_copy(rest_view.substr(position));

    auto& statement = result.arena.make<relational::UpdateStatement>();
    result.statement = &statement;

    auto& table_ref = result.arena.make<relational::TableReference>();
    table_ref.name = make_qualified_name(table_spec);
    statement.target = &table_ref;

    const auto eq_pos = assignment_text.find('=');
    if (eq_pos == std::string::npos) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "SET clause must contain '=' assignment";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Provide assignments in the form column = expression."};
        result.diagnostics.push_back(std::move(diagnostic));
        result.statement = nullptr;
        result.arena.reset();
        return result;
    }

    const auto target_column_name = trim_copy(assignment_text.substr(0, eq_pos));
    const auto rhs_text = trim_copy(assignment_text.substr(eq_pos + 1U));

    if (target_column_name.empty() || rhs_text.empty()) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "SET clause is missing a column or value";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Ensure the assignment specifies both a column and an expression."};
        result.diagnostics.push_back(std::move(diagnostic));
        result.statement = nullptr;
        result.arena.reset();
        return result;
    }

    relational::UpdateAssignment assignment{};
    assignment.column = make_identifier(target_column_name);

    static const std::regex kSetOperationPattern(R"(^([A-Za-z0-9_]+)\s*([+-])\s*([0-9]+)$)", std::regex::icase);
    std::smatch op_match;
    if (std::regex_match(rhs_text, op_match, kSetOperationPattern)) {
        const auto source_column = trim_copy(op_match[1].str());
        const auto op_token = op_match[2].str();
        const auto operand_literal = trim_copy(op_match[3].str());

        if (!iequals(source_column, target_column_name)) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "SET operations currently require the same source and target column";
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Use the form column = column +/- <integer>."};
            result.diagnostics.push_back(std::move(diagnostic));
            result.statement = nullptr;
            result.arena.reset();
            return result;
        }

        auto& left = make_identifier_expression(result.arena, source_column);
        auto& right = make_numeric_literal_expression(result.arena, operand_literal);
        auto& binary = make_binary_expression(result.arena,
                                              (op_token == "+") ? relational::BinaryOperator::Add : relational::BinaryOperator::Subtract,
                                              left,
                                              right);
        assignment.value = &binary;
    } else if (is_signed_integer_literal(rhs_text) || is_decimal_literal(rhs_text)) {
        assignment.value = &make_numeric_literal_expression(result.arena, rhs_text);
    } else if (!rhs_text.empty() && rhs_text.front() == '\'' && rhs_text.back() == '\'') {
        std::size_t literal_pos = 0U;
        std::string literal_value;
        std::string error;
        if (!parse_string_literal(rhs_text, literal_pos, literal_value, error) || literal_pos != rhs_text.size()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = std::move(error);
            diagnostic.statement = trim_copy(input);
            result.diagnostics.push_back(std::move(diagnostic));
            result.statement = nullptr;
            result.arena.reset();
            return result;
        }
        assignment.value = &make_string_literal_expression(result.arena, std::move(literal_value));
    } else if (iequals(rhs_text, "TRUE") || iequals(rhs_text, "FALSE")) {
        assignment.value = &make_boolean_literal_expression(result.arena, iequals(rhs_text, "TRUE"));
    } else {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Unsupported SET expression";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Supported forms: column = column +/- integer, column = integer, column = decimal, column = 'text', column = TRUE/FALSE."};
        result.diagnostics.push_back(std::move(diagnostic));
        result.statement = nullptr;
        result.arena.reset();
        return result;
    }

    statement.assignments.push_back(std::move(assignment));

    static const std::regex kWherePattern(R"(^([A-Za-z0-9_]+)\s*=\s*([0-9]+)$)", std::regex::icase);
    std::smatch where_match;
    if (!std::regex_match(where_text, where_match, kWherePattern)) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Unsupported WHERE clause";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Only equality against integer literals is supported."};
        result.diagnostics.push_back(std::move(diagnostic));
        result.statement = nullptr;
        result.arena.reset();
        return result;
    }

    const auto where_column_name = trim_copy(where_match[1].str());
    const auto where_literal = trim_copy(where_match[2].str());

    auto& left = make_identifier_expression(result.arena, where_column_name);
    auto& right = make_numeric_literal_expression(result.arena, where_literal);
    auto& predicate = make_binary_expression(result.arena, relational::BinaryOperator::Equal, left, right);
    statement.where = &predicate;

    return result;
}

DeleteParseResult parse_delete(std::string_view input)
{
    DeleteParseResult result{};
    const auto normalized = strip_leading_comments(input);
    const auto statement_text = strip_trailing_semicolon(normalized);
    if (statement_text.empty()) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "DELETE statement is empty";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Provide a DELETE FROM <table> WHERE ... statement."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    if (!starts_with_ci(statement_text, "DELETE")) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Only DELETE statements are supported";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Begin the command with the DELETE keyword."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    constexpr std::size_t kDeleteLength = 6U;
    if (statement_text.size() <= kDeleteLength) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "DELETE statement must specify FROM <table>";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Use DELETE FROM <table> WHERE ..."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    auto rest = trim_copy(statement_text.substr(kDeleteLength));
    if (!starts_with_ci(rest, "FROM")) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "DELETE must specify FROM <table>";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Use DELETE FROM <table> WHERE ..."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }
    rest = trim_copy(rest.substr(4));

    std::size_t position = 0U;
    auto rest_view = std::string_view(rest);
    skip_whitespace(rest_view, position);
    const auto table_start = position;
    while (position < rest_view.size() && !std::isspace(static_cast<unsigned char>(rest_view[position]))) {
        ++position;
    }
    if (table_start == position) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "DELETE requires a table name";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Specify the table to delete from after FROM."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }
    const auto table_spec = trim_copy(rest_view.substr(table_start, position - table_start));
    skip_whitespace(rest_view, position);

    if (position + 5U > rest_view.size() || uppercase_copy(rest_view.substr(position, 5U)) != "WHERE") {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "DELETE requires a WHERE clause";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Add WHERE <column> = <value> to control which rows are deleted."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }
    position += 5U;
    skip_whitespace(rest_view, position);
    auto where_text = trim_copy(rest_view.substr(position));

    static const std::regex kWherePattern(R"(^([A-Za-z0-9_]+)\s*=\s*([0-9]+)$)", std::regex::icase);
    std::smatch where_match;
    if (!std::regex_match(where_text, where_match, kWherePattern)) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Unsupported WHERE clause";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Only equality against integer literals is supported."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    auto& statement = result.arena.make<relational::DeleteStatement>();
    result.statement = &statement;

    auto& table_ref = result.arena.make<relational::TableReference>();
    table_ref.name = make_qualified_name(table_spec);
    statement.target = &table_ref;

    const auto where_column_name = trim_copy(where_match[1].str());
    const auto where_literal = trim_copy(where_match[2].str());
    auto& left = make_identifier_expression(result.arena, where_column_name);
    auto& right = make_numeric_literal_expression(result.arena, where_literal);
    auto& predicate = make_binary_expression(result.arena, relational::BinaryOperator::Equal, left, right);
    statement.where = &predicate;

    return result;
}

};

struct kw_on : keyword<'O', 'N'> {
};

struct kw_where : keyword<'W', 'H', 'E', 'R', 'E'> {
};

struct kw_group : keyword<'G', 'R', 'O', 'U', 'P'> {
};

struct kw_order : keyword<'O', 'R', 'D', 'E', 'R'> {
};

struct kw_by : keyword<'B', 'Y'> {
};

struct kw_limit : keyword<'L', 'I', 'M', 'I', 'T'> {
};

struct kw_offset : keyword<'O', 'F', 'F', 'S', 'E', 'T'> {
};

struct kw_true_literal : keyword<'T', 'R', 'U', 'E'> {
};

struct kw_false_literal : keyword<'F', 'A', 'L', 'S', 'E'> {
};

struct kw_null_literal : keyword<'N', 'U', 'L', 'L'> {
};

struct kw_asc : keyword<'A', 'S', 'C'> {
};

struct kw_desc : keyword<'D', 'E', 'S', 'C'> {
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

struct kw_constraint : keyword<'C', 'O', 'N', 'S', 'T', 'R', 'A', 'I', 'N', 'T'> {
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

struct schema_if_exists_rule : if_exists_rule {
};

struct schema_entry_rule
    : pegtl::seq<optional_space,
                 pegtl::opt<pegtl::seq<schema_if_exists_rule, required_space>>,
                 schema_name_rule> {
};

struct schema_name_list_rule
    : pegtl::seq<schema_entry_rule,
                 pegtl::star<pegtl::seq<optional_space, pegtl::one<','>, schema_entry_rule>>> {
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

struct default_identifier_rule : identifier_rule {
};

struct default_expression_rule;
struct default_function_argument_list_rule;

struct default_parenthesized_expression_rule
    : pegtl::seq<left_paren, optional_space, default_expression_rule, optional_space, right_paren> {
};

struct default_function_argument_list_rule
    : pegtl::seq<default_expression_rule,
                 pegtl::star<optional_space, pegtl::one<','>, optional_space, default_expression_rule>> {
};

struct default_function_call_rule
    : pegtl::seq<default_identifier_rule,
                 optional_space,
                 left_paren,
                 optional_space,
                 pegtl::opt<default_function_argument_list_rule>,
                 optional_space,
                 right_paren> {
};

struct default_term_rule
    : pegtl::sor<expr::string_literal,
                 expr::numeric_literal,
                 default_function_call_rule,
                 default_parenthesized_expression_rule,
                 default_identifier_rule> {
};

struct default_operator_token : pegtl::sor<pegtl::one<'+'>, pegtl::one<'-'>, pegtl::one<'*'>, pegtl::one<'/'>> {
};

struct default_operator_sequence_rule
    : pegtl::seq<optional_space, default_operator_token, optional_space, default_term_rule> {
};

struct default_expression_rule
    : pegtl::seq<default_term_rule, pegtl::star<default_operator_sequence_rule>> {
};

struct default_clause_rule
    : pegtl::seq<kw_default, required_space, default_expression_rule> {
};

struct primary_key_rule : pegtl::seq<kw_primary, required_space, kw_key> {
};

struct unique_constraint_rule : kw_unique {
};

struct constraint_identifier_rule : identifier_rule {
};

struct column_constraint_rule
    : pegtl::seq<required_space,
                 pegtl::opt<pegtl::seq<kw_constraint,
                                      required_space,
                                      constraint_identifier_rule,
                                      required_space>>,
                 pegtl::sor<default_clause_rule,
                            constraint_not_null_rule,
                            primary_key_rule,
                            unique_constraint_rule>> {
};

struct column_definition_rule
    : pegtl::seq<column_identifier,
                 required_space,
                 type_identifier,
                 pegtl::star<column_constraint_rule>,
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

bool starts_with_ci(std::string_view text, std::string_view prefix)
{
    if (prefix.size() > text.size()) {
        return false;
    }
    for (std::size_t index = 0; index < prefix.size(); ++index) {
        const auto left = static_cast<unsigned char>(text[index]);
        const auto right = static_cast<unsigned char>(prefix[index]);
        if (std::toupper(left) != std::toupper(right)) {
            return false;
        }
    }
    return true;
}

std::string strip_leading_comments(std::string_view text)
{
    std::size_t position = 0U;
    while (position < text.size()) {
        while (position < text.size() && std::isspace(static_cast<unsigned char>(text[position])) != 0) {
            ++position;
        }

        if (position + 1U < text.size() && text[position] == '-' && text[position + 1U] == '-') {
            position += 2U;
            while (position < text.size() && text[position] != '\n' && text[position] != '\r') {
                ++position;
            }
            continue;
        }

        if (position + 1U < text.size() && text[position] == '/' && text[position + 1U] == '*') {
            position += 2U;
            while (position + 1U < text.size() && !(text[position] == '*' && text[position + 1U] == '/')) {
                ++position;
            }
            if (position + 1U < text.size()) {
                position += 2U;
            }
            continue;
        }

        break;
    }

    return trim_copy(text.substr(position));
}

std::string uppercase_copy(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (unsigned char ch : text) {
        result.push_back(static_cast<char>(std::toupper(ch)));
    }
    return result;
}

std::string strip_trailing_semicolon(std::string_view text)
{
    auto stripped = trim_copy(text);
    if (!stripped.empty() && stripped.back() == ';') {
        stripped.pop_back();
        stripped = trim_copy(stripped);
    }
    return stripped;
}

void skip_whitespace(std::string_view text, std::size_t& position)
{
    while (position < text.size() && std::isspace(static_cast<unsigned char>(text[position])) != 0) {
        ++position;
    }
}

bool parse_string_literal(std::string_view text, std::size_t& position, std::string& out, std::string& error)
{
    if (position >= text.size() || text[position] != '\'') {
        error = "Expected string literal.";
        return false;
    }
    ++position;
    std::string buffer;
    while (position < text.size()) {
        const char ch = text[position];
        if (ch == '\'') {
            if (position + 1U < text.size() && text[position + 1U] == '\'') {
                buffer.push_back('\'');
                position += 2U;
                continue;
            }
            ++position;
            out = std::move(buffer);
            return true;
        }
        buffer.push_back(ch);
        ++position;
    }
    error = "Unterminated string literal.";
    return false;
}

bool parse_identifier_list(std::string_view text, std::vector<std::string>& out)
{
    out.clear();
    std::size_t position = 0U;
    while (position < text.size()) {
        skip_whitespace(text, position);
        if (position >= text.size()) {
            break;
        }
        std::size_t start = position;
        while (position < text.size() && text[position] != ',') {
            ++position;
        }
        auto token = trim_copy(text.substr(start, position - start));
        if (!token.empty()) {
            out.push_back(std::move(token));
        }
        if (position < text.size() && text[position] == ',') {
            ++position;
        }
    }
    return !out.empty();
}

bool is_signed_integer_literal(std::string_view token)
{
    if (token.empty()) {
        return false;
    }
    std::size_t index = 0U;
    if (token[index] == '+' || token[index] == '-') {
        ++index;
    }
    if (index >= token.size()) {
        return false;
    }
    for (; index < token.size(); ++index) {
        if (!std::isdigit(static_cast<unsigned char>(token[index]))) {
            return false;
        }
    }
    return true;
}

std::size_t find_keyword_ci(std::string_view text, std::string_view keyword)
{
    const auto haystack = uppercase_copy(text);
    const auto needle = uppercase_copy(keyword);
    return haystack.find(needle);
}

bool is_decimal_literal(std::string_view token)
{
    if (token.empty()) {
        return false;
    }
    std::size_t index = 0U;
    if (token[index] == '+' || token[index] == '-') {
        ++index;
    }
    bool digit_seen = false;
    bool dot_seen = false;
    for (; index < token.size(); ++index) {
        const char ch = token[index];
        if (std::isdigit(static_cast<unsigned char>(ch)) != 0) {
            digit_seen = true;
            continue;
        }
        if (ch == '.' && !dot_seen) {
            dot_seen = true;
            continue;
        }
        return false;
    }
    return digit_seen && dot_seen;
}

bool populate_insert_values(std::string_view text,
                            relational::AstArena& arena,
                            relational::InsertStatement& statement,
                            std::string& error,
                            std::vector<std::string>& hints)
{
    statement.rows.clear();
    std::size_t position = 0U;
    while (position < text.size()) {
        skip_whitespace(text, position);
        if (position >= text.size()) {
            break;
        }
        if (text[position] != '(') {
            error = "VALUES list must start with '('";
            return false;
        }
        ++position;
        relational::InsertRow row{};
        while (true) {
            skip_whitespace(text, position);
            if (position >= text.size()) {
                error = "Unexpected end of VALUES list.";
                return false;
            }

            if (text[position] == '\'') {
                std::string literal;
                if (!parse_string_literal(text, position, literal, error)) {
                    return false;
                }
                auto& expression = arena.make<relational::LiteralExpression>();
                expression.tag = relational::LiteralTag::String;
                expression.text = std::move(literal);
                row.values.push_back(&expression);
            } else {
                const std::size_t begin = position;
                while (position < text.size() && text[position] != ',' && text[position] != ')') {
                    ++position;
                }
                auto token = trim_copy(text.substr(begin, position - begin));
                if (token.empty()) {
                    error = "Empty literal in VALUES list.";
                    return false;
                }

                auto upper = uppercase_copy(token);
                auto& expression = arena.make<relational::LiteralExpression>();
                if (iequals(upper, "NULL")) {
                    expression.tag = relational::LiteralTag::Null;
                    expression.text.clear();
                } else if (iequals(upper, "TRUE") || iequals(upper, "FALSE")) {
                    expression.tag = relational::LiteralTag::Boolean;
                    expression.boolean_value = iequals(upper, "TRUE");
                    expression.text = std::move(token);
                } else if (is_decimal_literal(token)) {
                    expression.tag = relational::LiteralTag::Decimal;
                    expression.text = std::move(token);
                } else if (is_signed_integer_literal(token)) {
                    expression.tag = relational::LiteralTag::Integer;
                    expression.text = std::move(token);
                } else {
                    expression.tag = relational::LiteralTag::String;
                    expression.text = std::move(token);
                }
                row.values.push_back(&expression);
            }

            skip_whitespace(text, position);
            if (position >= text.size()) {
                error = "Unterminated VALUES row.";
                return false;
            }
            if (text[position] == ',') {
                ++position;
                continue;
            }
            if (text[position] == ')') {
                ++position;
                break;
            }
            error = "Unexpected token in VALUES list.";
            return false;
        }

        statement.rows.push_back(std::move(row));
        skip_whitespace(text, position);
        if (position < text.size() && text[position] == ',') {
            ++position;
            continue;
        }
        break;
    }

    skip_whitespace(text, position);
    if (position != text.size()) {
        error = "Unexpected trailing characters after VALUES list.";
        return false;
    }

    if (statement.rows.empty()) {
        error = "VALUES list is empty.";
        hints = {"Supply at least one parenthesized row in the VALUES clause."};
        return false;
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
    diagnostic.severity = ParserSeverity::Warning;
    diagnostic.message = format_parse_message(error.message());
    diagnostic.statement = trim_copy(source);
    diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};

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
                                         std::string_view source,
                                         std::vector<ParserDiagnostic>& diagnostics)
{
    std::unordered_set<std::string> seen{};
    for (const auto& column : statement.columns) {
        auto [_, inserted] = seen.insert(column.name.value);
        if (!inserted) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "Duplicate column name '" + column.name.value + "'";
            diagnostic.statement = trim_copy(source);
            diagnostic.remediation_hints = {"Remove or rename the duplicate column before retrying the statement."};
            diagnostics.push_back(std::move(diagnostic));
        }
    }
}

void append_duplicate_schema_diagnostics(const DropSchemaStatement& statement,
                                         std::string_view source,
                                         std::vector<ParserDiagnostic>& diagnostics)
{
    std::unordered_map<std::string, std::size_t> seen{};
    for (std::size_t index = 0; index < statement.schemas.size(); ++index) {
        const auto& schema = statement.schemas[index];
        std::string key = schema.database.value.empty() ? schema.name.value
                                                        : schema.database.value + "." + schema.name.value;
        auto [it, inserted] = seen.emplace(key, index);
        if (!inserted) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Warning;
            const auto first_position = it->second + 1U;
            const auto duplicate_position = index + 1U;
            diagnostic.message = "Duplicate schema '" + key + "' at positions "
                                 + std::to_string(first_position) + " and " + std::to_string(duplicate_position);
            diagnostic.statement = trim_copy(source);
            diagnostic.remediation_hints = {"Remove duplicates or de-duplicate schema names when issuing DROP SCHEMA commands."};
            diagnostics.push_back(std::move(diagnostic));
        }
    }
}

std::size_t find_statement_terminator(std::string_view input, std::size_t start)
{
    bool in_single_quote = false;
    bool in_double_quote = false;
    bool in_line_comment = false;
    bool in_block_comment = false;

    for (std::size_t index = start; index < input.size(); ++index) {
        const char ch = input[index];

        if (in_line_comment) {
            if (ch == '\n') {
                in_line_comment = false;
            }
            continue;
        }

        if (in_block_comment) {
            if (ch == '*' && index + 1U < input.size() && input[index + 1U] == '/') {
                in_block_comment = false;
                ++index;
            }
            continue;
        }

        if (in_single_quote) {
            if (ch == '\'' && (index + 1U >= input.size() || input[index + 1U] != '\'')) {
                in_single_quote = false;
            } else if (ch == '\'' && index + 1U < input.size()) {
                ++index;
            }
            continue;
        }

        if (in_double_quote) {
            if (ch == '"' && (index + 1U >= input.size() || input[index + 1U] != '"')) {
                in_double_quote = false;
            } else if (ch == '"' && index + 1U < input.size()) {
                ++index;
            }
            continue;
        }

        if (ch == '-' && index + 1U < input.size() && input[index + 1U] == '-') {
            in_line_comment = true;
            ++index;
            continue;
        }

        if (ch == '/' && index + 1U < input.size() && input[index + 1U] == '*') {
            in_block_comment = true;
            ++index;
            continue;
        }

        if (ch == '\'') {
            in_single_quote = true;
            continue;
        }

        if (ch == '"') {
            in_double_quote = true;
            continue;
        }

        if (ch == ';') {
            return index;
        }
    }

    return std::string_view::npos;
}

struct DropSchemaParseState final {
    bool next_if_exists = false;
};

struct CreateTableParseState final {
    std::optional<Identifier> pending_constraint_name{};
};

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
                diagnostic.statement = trimmed;
                diagnostic.remediation_hints = {"Capture this embedded statement and file a parser bug report."};
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
                diagnostic.statement = trimmed;
                diagnostic.remediation_hints = {"Capture this embedded statement and file a parser bug report."};
                diagnostics.push_back(std::move(diagnostic));
            }
            continue;
        }

        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Warning;
        diagnostic.message = "Unsupported embedded CREATE statement '" + trimmed + "'";
        diagnostic.statement = trimmed;
        diagnostic.remediation_hints = {"Only CREATE TABLE and CREATE VIEW statements are supported inside CREATE SCHEMA blocks."};
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
    static void apply(const Input&, DropSchemaStatement&, DropSchemaParseState&)
    {
    }
};

template <>
struct drop_schema_action<if_exists_rule> {
    template <typename Input>
    static void apply(const Input&, DropSchemaStatement& statement, DropSchemaParseState&)
    {
        statement.if_exists = true;
    }
};

template <>
struct drop_schema_action<schema_if_exists_rule> {
    template <typename Input>
    static void apply(const Input&, DropSchemaStatement&, DropSchemaParseState& state)
    {
        state.next_if_exists = true;
    }
};

template <>
struct drop_schema_action<schema_name_head> {
    template <typename Input>
    static void apply(const Input& in, DropSchemaStatement& statement, DropSchemaParseState& state)
    {
        auto& schema = statement.schemas.emplace_back();
        schema.database.value.clear();
        schema.name.value = in.string();
        schema.if_exists = state.next_if_exists;
        state.next_if_exists = false;
    }
};

template <>
struct drop_schema_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, DropSchemaStatement& statement, DropSchemaParseState&)
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
    static void apply(const Input&, DropSchemaStatement& statement, DropSchemaParseState&)
    {
        statement.behavior = DropSchemaStatement::Behavior::Cascade;
    }
};

template <>
struct drop_schema_action<kw_restrict> {
    template <typename Input>
    static void apply(const Input&, DropSchemaStatement& statement, DropSchemaParseState&)
    {
        statement.behavior = DropSchemaStatement::Behavior::Restrict;
    }
};

template <typename Rule>
struct create_table_action {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement&, CreateTableParseState&)
    {
    }
};

template <>
struct create_table_action<if_not_exists_rule> {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement& statement, CreateTableParseState&)
    {
        statement.if_not_exists = true;
    }
};

template <>
struct create_table_action<schema_name_head> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement, CreateTableParseState&)
    {
        statement.schema.value.clear();
        statement.name.value = in.string();
    }
};

template <>
struct create_table_action<schema_name_tail> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement, CreateTableParseState&)
    {
        statement.schema.value = statement.name.value;
        statement.name.value = in.string();
    }
};

template <>
struct create_table_action<column_identifier> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement, CreateTableParseState& state)
    {
        auto& column = statement.columns.emplace_back();
        column.name.value = in.string();
        column.not_null = false;
        column.primary_key = false;
        column.unique = false;
        column.default_expression.reset();
        column.default_constraint_name.reset();
        column.not_null_constraint_name.reset();
        column.primary_key_constraint_name.reset();
        column.unique_constraint_name.reset();
        state.pending_constraint_name.reset();
    }
};

template <>
struct create_table_action<type_identifier> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement, CreateTableParseState&)
    {
        if (!statement.columns.empty()) {
            statement.columns.back().type_name.value = in.string();
        }
    }
};

template <>
struct create_table_action<constraint_identifier_rule> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement&, CreateTableParseState& state)
    {
        state.pending_constraint_name = Identifier{};
        state.pending_constraint_name->value = in.string();
    }
};

template <>
struct create_table_action<constraint_not_null_rule> {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement& statement, CreateTableParseState& state)
    {
        if (!statement.columns.empty()) {
            auto& column = statement.columns.back();
            column.not_null = true;
            if (state.pending_constraint_name) {
                column.not_null_constraint_name = std::move(*state.pending_constraint_name);
            }
        }
        state.pending_constraint_name.reset();
    }
};

template <>
struct create_table_action<default_expression_rule> {
    template <typename Input>
    static void apply(const Input& in, CreateTableStatement& statement, CreateTableParseState& state)
    {
        if (!statement.columns.empty()) {
            auto& column = statement.columns.back();
            column.default_expression = trim_copy(in.string());
            if (state.pending_constraint_name) {
                column.default_constraint_name = std::move(*state.pending_constraint_name);
            }
        }
        state.pending_constraint_name.reset();
    }
};

template <>
struct create_table_action<primary_key_rule> {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement& statement, CreateTableParseState& state)
    {
        if (!statement.columns.empty()) {
            auto& column = statement.columns.back();
            column.primary_key = true;
            column.not_null = true;
            if (state.pending_constraint_name) {
                column.primary_key_constraint_name = std::move(*state.pending_constraint_name);
            }
        }
        state.pending_constraint_name.reset();
    }
};

template <>
struct create_table_action<unique_constraint_rule> {
    template <typename Input>
    static void apply(const Input&, CreateTableStatement& statement, CreateTableParseState& state)
    {
        if (!statement.columns.empty()) {
            auto& column = statement.columns.back();
            column.unique = true;
            if (state.pending_constraint_name) {
                column.unique_constraint_name = std::move(*state.pending_constraint_name);
            }
        }
        state.pending_constraint_name.reset();
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

struct SelectParseState final {
    relational::AstArena* arena = nullptr;
    relational::SelectStatement* statement = nullptr;
    relational::QuerySpecification* query = nullptr;
    relational::SelectItem* current_select_item = nullptr;
    relational::TableReference* current_table = nullptr;
    std::size_t current_table_index = 0U;
    std::size_t last_table_index = 0U;
    bool has_last_table = false;
    std::optional<std::size_t> last_join_index{};
    relational::JoinType join_type_override = relational::JoinType::Inner;
    bool join_type_override_set = false;
    struct PendingJoin final {
        relational::JoinType type = relational::JoinType::Inner;
        relational::JoinClause::InputKind left_kind = relational::JoinClause::InputKind::Table;
        std::size_t left_index = 0U;
        std::size_t right_index = 0U;
        bool has_right = false;
    };
    std::optional<PendingJoin> pending_join{};
    relational::Expression* pending_join_predicate = nullptr;
    relational::OrderByItem* current_order_item = nullptr;
    relational::LimitClause* limit_clause = nullptr;
    std::vector<relational::Expression*> expression_stack{};
    relational::BinaryOperator pending_operator = relational::BinaryOperator::Equal;
    bool has_pending_operator = false;
    relational::QualifiedName star_qualifier{};
};

Identifier make_identifier(std::string_view text)
{
    Identifier identifier{};
    identifier.value = trim_copy(text);
    return identifier;
}

relational::QualifiedName make_qualified_name(std::string_view text)
{
    relational::QualifiedName name{};
    std::string part{};
    for (const char ch : text) {
        if (ch == '.') {
            if (!part.empty()) {
                Identifier identifier{};
                identifier.value = part;
                name.parts.push_back(std::move(identifier));
                part.clear();
            }
            continue;
        }

        if (!std::isspace(static_cast<unsigned char>(ch))) {
            part.push_back(ch);
        }
    }

    if (!part.empty()) {
        Identifier identifier{};
        identifier.value = std::move(part);
        name.parts.push_back(std::move(identifier));
    }

    return name;
}

std::string unescape_string_literal(std::string_view text)
{
    std::string result{};
    if (text.size() >= 2U && text.front() == '\'' && text.back() == '\'') {
        for (std::size_t index = 1U; index + 1U < text.size(); ++index) {
            const char ch = text[index];
            if (ch == '\'' && index + 1U < text.size() - 1U && text[index + 1U] == '\'') {
                result.push_back('\'');
                ++index;
            } else {
                result.push_back(ch);
            }
        }
        return result;
    }

    result.assign(text.begin(), text.end());
    return result;
}

template <typename State>
void push_expression(State& state, relational::Expression& expression)
{
    state.expression_stack.push_back(&expression);
}

template <typename State>
relational::Expression* pop_expression(State& state)
{
    if (state.expression_stack.empty()) {
        return nullptr;
    }

    auto* expression = state.expression_stack.back();
    state.expression_stack.pop_back();
    return expression;
}

struct select_identifier_rule
    : pegtl::seq<identifier_rule,
                 pegtl::star<pegtl::seq<optional_space, dot, optional_space, identifier_rule>>> {
};

struct select_column_identifier_rule : select_identifier_rule {
};

struct select_table_identifier_rule : select_identifier_rule {
};

struct select_star_qualifier_rule : select_identifier_rule {
};

struct select_alias_identifier_rule : identifier_rule {
};

struct select_table_alias_identifier_rule : identifier_rule {
};

struct select_string_literal_rule : expr::string_literal {
};

struct select_numeric_literal_rule : expr::numeric_literal {
};

struct select_boolean_true_rule : kw_true_literal {
};

struct select_boolean_false_rule : kw_false_literal {
};

struct select_boolean_literal_rule : pegtl::sor<select_boolean_true_rule, select_boolean_false_rule> {
};

struct select_null_literal_rule : kw_null_literal {
};

struct select_star_token : pegtl::one<'*'> {
};

struct select_star_expression_rule
    : pegtl::sor<pegtl::seq<select_star_qualifier_rule, optional_space, dot, optional_space, select_star_token>,
                 select_star_token> {
};

struct select_literal_rule
    : pegtl::sor<select_null_literal_rule,
                 select_boolean_literal_rule,
                 select_numeric_literal_rule,
                 select_string_literal_rule> {
};

struct select_primary_expression_rule
    : pegtl::sor<select_literal_rule, select_star_expression_rule, select_column_identifier_rule> {
};

struct select_not_equal_operator_rule : pegtl::string<'<', '>'> {
};

struct select_less_equal_operator_rule : pegtl::string<'<', '='> {
};

struct select_greater_equal_operator_rule : pegtl::string<'>', '='> {
};

struct select_less_operator_rule : pegtl::one<'<'> {
};

struct select_greater_operator_rule : pegtl::one<'>'> {
};

struct select_equal_operator_rule : pegtl::one<'='> {
};

struct select_comparison_operator_rule
    : pegtl::sor<select_less_equal_operator_rule,
                 select_greater_equal_operator_rule,
                 select_not_equal_operator_rule,
                 select_less_operator_rule,
                 select_greater_operator_rule,
                 select_equal_operator_rule> {
};

struct select_comparison_expression_rule
    : pegtl::seq<select_primary_expression_rule,
                 optional_space,
                 select_comparison_operator_rule,
                 optional_space,
                 select_primary_expression_rule> {
};

struct select_expression_rule
    : pegtl::sor<select_comparison_expression_rule, select_primary_expression_rule> {
};

struct select_value_expression_rule : select_expression_rule {
};

struct where_expression_rule : select_expression_rule {
};

struct group_expression_rule : select_expression_rule {
};

struct order_expression_rule : select_expression_rule {
};

struct limit_row_expression_rule : select_expression_rule {
};

struct limit_offset_expression_rule : select_expression_rule {
};

struct select_alias_rule
    : pegtl::seq<kw_as, required_space, select_alias_identifier_rule> {
};

struct select_item_rule
    : pegtl::seq<select_value_expression_rule, pegtl::opt<required_space, select_alias_rule>> {
};

struct select_list_rule
    : pegtl::seq<select_item_rule,
                 pegtl::star<optional_space, comma, optional_space, select_item_rule>> {
};

struct join_inner_keyword : kw_inner {
};

struct join_left_keyword : kw_left {
};

struct join_right_keyword : kw_right {
};

struct join_full_keyword : kw_full {
};

struct join_outer_keyword : kw_outer {
};

struct join_cross_keyword : kw_cross {
};

struct join_on_keyword : kw_on {
};

struct select_table_factor_rule
    : pegtl::seq<select_table_identifier_rule,
                 pegtl::opt<required_space,
                            pegtl::seq<kw_as, required_space, select_table_alias_identifier_rule>>> {
};

struct select_join_type_inner_rule : join_inner_keyword {
};

struct select_join_type_left_rule
    : pegtl::seq<join_left_keyword, pegtl::opt<required_space, join_outer_keyword>> {
};

struct select_join_type_right_rule
    : pegtl::seq<join_right_keyword, pegtl::opt<required_space, join_outer_keyword>> {
};

struct select_join_type_full_rule
    : pegtl::seq<join_full_keyword, pegtl::opt<required_space, join_outer_keyword>> {
};

struct select_join_type_cross_rule : join_cross_keyword {
};

struct select_join_type_rule
    : pegtl::sor<select_join_type_inner_rule,
                 select_join_type_left_rule,
                 select_join_type_right_rule,
                 select_join_type_full_rule,
                 select_join_type_cross_rule> {
};

struct select_join_type_with_space_rule
    : pegtl::seq<select_join_type_rule, required_space> {
};

struct select_join_condition_rule : select_expression_rule {
};

struct select_join_clause_rule
    : pegtl::seq<pegtl::opt<select_join_type_with_space_rule>,
                 kw_join,
                 required_space,
                 select_table_factor_rule,
                 pegtl::opt<required_space, join_on_keyword, required_space, select_join_condition_rule>> {
};

struct select_table_reference_rule
    : pegtl::seq<select_table_factor_rule,
                 pegtl::star<optional_space, select_join_clause_rule>> {
};

struct select_table_reference_list_rule
    : pegtl::seq<select_table_reference_rule,
                 pegtl::star<optional_space, comma, optional_space, select_table_reference_rule>> {
};

struct from_clause_rule : pegtl::seq<kw_from, required_space, select_table_reference_list_rule> {
};

struct where_clause_rule : pegtl::seq<kw_where, required_space, where_expression_rule> {
};

struct order_desc_keyword : kw_desc {
};

struct order_asc_keyword : kw_asc {
};

struct order_direction_rule : pegtl::sor<order_asc_keyword, order_desc_keyword> {
};

struct order_item_rule
    : pegtl::seq<order_expression_rule, pegtl::opt<required_space, order_direction_rule>> {
};

struct order_list_rule
    : pegtl::seq<order_item_rule,
                 pegtl::star<optional_space, comma, optional_space, order_item_rule>> {
};

struct group_list_rule
    : pegtl::seq<group_expression_rule,
                 pegtl::star<optional_space, comma, optional_space, group_expression_rule>> {
};

struct group_by_clause_rule : pegtl::seq<kw_group, required_space, kw_by, required_space, group_list_rule> {
};

struct order_by_clause_rule
    : pegtl::seq<kw_order, required_space, kw_by, required_space, order_list_rule> {
};

struct limit_clause_rule
    : pegtl::seq<kw_limit,
                 required_space,
                 limit_row_expression_rule,
                 pegtl::opt<required_space, kw_offset, required_space, limit_offset_expression_rule>> {
};

struct select_statement_grammar
    : pegtl::seq<optional_space,
                 kw_select,
                 pegtl::opt<required_space, kw_distinct>,
                 required_space,
                 select_list_rule,
                 pegtl::opt<required_space, from_clause_rule>,
                 pegtl::opt<required_space, where_clause_rule>,
                 pegtl::opt<required_space, group_by_clause_rule>,
                 pegtl::opt<required_space, order_by_clause_rule>,
                 pegtl::opt<required_space, limit_clause_rule>,
                 optional_space,
                 pegtl::opt<semicolon, optional_space>,
                 pegtl::eof> {
};

template <typename Rule>
struct select_action {
    template <typename Input>
    static void apply(const Input&, SelectParseState&)
    {
    }
};

template <>
struct select_action<kw_distinct> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.query != nullptr) {
            state.query->distinct = true;
        }
    }
};

template <>
struct select_action<select_string_literal_rule> {
    template <typename Input>
    static void apply(const Input& in, SelectParseState& state)
    {
        if (state.arena == nullptr) {
            return;
        }

        auto& literal = state.arena->make<relational::LiteralExpression>();
        literal.tag = relational::LiteralTag::String;
        literal.text = unescape_string_literal(in.string());
        push_expression(state, literal);
    }
};

template <>
struct select_action<select_numeric_literal_rule> {
    template <typename Input>
    static void apply(const Input& in, SelectParseState& state)
    {
        if (state.arena == nullptr) {
            return;
        }

        auto text = trim_copy(in.string());
        auto& literal = state.arena->make<relational::LiteralExpression>();
        if (text.find_first_of(".eE") != std::string::npos) {
            literal.tag = relational::LiteralTag::Decimal;
        } else {
            literal.tag = relational::LiteralTag::Integer;
        }
        literal.text = std::move(text);
        push_expression(state, literal);
    }
};

template <>
struct select_action<select_boolean_true_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr) {
            return;
        }

        auto& literal = state.arena->make<relational::LiteralExpression>();
        literal.tag = relational::LiteralTag::Boolean;
        literal.boolean_value = true;
        literal.text = "TRUE";
        push_expression(state, literal);
    }
};

template <>
struct select_action<select_boolean_false_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr) {
            return;
        }

        auto& literal = state.arena->make<relational::LiteralExpression>();
        literal.tag = relational::LiteralTag::Boolean;
        literal.boolean_value = false;
        literal.text = "FALSE";
        push_expression(state, literal);
    }
};

template <>
struct select_action<select_null_literal_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr) {
            return;
        }

        auto& literal = state.arena->make<relational::LiteralExpression>();
        literal.tag = relational::LiteralTag::Null;
        literal.text = "NULL";
        push_expression(state, literal);
    }
};

template <>
struct select_action<select_column_identifier_rule> {
    template <typename Input>
    static void apply(const Input& in, SelectParseState& state)
    {
        if (state.arena == nullptr) {
            return;
        }

        auto& identifier = state.arena->make<relational::IdentifierExpression>();
        identifier.name = make_qualified_name(in.string());
        push_expression(state, identifier);
    }
};

template <>
struct select_action<select_star_qualifier_rule> {
    template <typename Input>
    static void apply(const Input& in, SelectParseState& state)
    {
        state.star_qualifier = make_qualified_name(in.string());
    }
};

template <>
struct select_action<select_star_token> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr) {
            return;
        }

        auto& star = state.arena->make<relational::StarExpression>();
        star.qualifier = state.star_qualifier;
        state.star_qualifier.parts.clear();
        push_expression(state, star);
    }
};

template <>
struct select_action<select_comparison_operator_rule> {
    template <typename Input>
    static void apply(const Input& in, SelectParseState& state)
    {
        const auto op = in.string();
        if (op == "=") {
            state.pending_operator = relational::BinaryOperator::Equal;
        } else if (op == "<>") {
            state.pending_operator = relational::BinaryOperator::NotEqual;
        } else if (op == "<") {
            state.pending_operator = relational::BinaryOperator::Less;
        } else if (op == "<=") {
            state.pending_operator = relational::BinaryOperator::LessOrEqual;
        } else if (op == ">") {
            state.pending_operator = relational::BinaryOperator::Greater;
        } else if (op == ">=") {
            state.pending_operator = relational::BinaryOperator::GreaterOrEqual;
        }
        state.has_pending_operator = true;
    }
};

template <>
struct select_action<select_comparison_expression_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr || !state.has_pending_operator) {
            state.expression_stack.clear();
            state.has_pending_operator = false;
            return;
        }

        auto* right = pop_expression(state);
        auto* left = pop_expression(state);
        if (left == nullptr || right == nullptr) {
            state.expression_stack.clear();
            state.has_pending_operator = false;
            return;
        }

        auto& binary = state.arena->make<relational::BinaryExpression>();
        binary.op = state.pending_operator;
        binary.left = left;
        binary.right = right;
        push_expression(state, binary);
        state.has_pending_operator = false;
    }
};

template <>
struct select_action<select_value_expression_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr || state.query == nullptr) {
            state.expression_stack.clear();
            return;
        }

        auto* expression = pop_expression(state);
        if (expression == nullptr) {
            state.expression_stack.clear();
            return;
        }

        auto& item = state.arena->make<relational::SelectItem>();
        item.expression = expression;
        state.query->select_items.push_back(&item);
        state.current_select_item = &item;
        state.expression_stack.clear();
    }
};

template <>
struct select_action<select_item_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        state.current_select_item = nullptr;
    }
};

template <>
struct select_action<select_alias_identifier_rule> {
    template <typename Input>
    static void apply(const Input& in, SelectParseState& state)
    {
        if (state.current_select_item != nullptr) {
            state.current_select_item->alias = make_identifier(in.string());
        }
    }
};

template <>
struct select_action<select_table_identifier_rule> {
    template <typename Input>
    static void apply(const Input& in, SelectParseState& state)
    {
        if (state.arena == nullptr || state.query == nullptr) {
            return;
        }

        auto& table = state.arena->make<relational::TableReference>();
        table.name = make_qualified_name(in.string());
        if (state.query->from == nullptr) {
            state.query->from = &table;
        }
        state.query->from_tables.push_back(&table);
        state.current_table = &table;
        if (!state.query->from_tables.empty()) {
            state.current_table_index = state.query->from_tables.size() - 1U;
            state.last_table_index = state.current_table_index;
            state.has_last_table = true;
            if (!state.pending_join.has_value()) {
                state.last_join_index.reset();
            }
            if (state.pending_join.has_value() && !state.pending_join->has_right) {
                state.pending_join->right_index = state.current_table_index;
                state.pending_join->has_right = true;
            }
        }
    }
};

template <>
struct select_action<select_table_alias_identifier_rule> {
    template <typename Input>
    static void apply(const Input& in, SelectParseState& state)
    {
        if (state.current_table != nullptr) {
            state.current_table->alias = make_identifier(in.string());
        }
    }
};

template <>
struct select_action<join_inner_keyword> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        state.join_type_override = relational::JoinType::Inner;
        state.join_type_override_set = true;
    }
};

template <>
struct select_action<join_left_keyword> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        state.join_type_override = relational::JoinType::LeftOuter;
        state.join_type_override_set = true;
    }
};

template <>
struct select_action<join_right_keyword> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        state.join_type_override = relational::JoinType::RightOuter;
        state.join_type_override_set = true;
    }
};

template <>
struct select_action<join_full_keyword> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        state.join_type_override = relational::JoinType::FullOuter;
        state.join_type_override_set = true;
    }
};

template <>
struct select_action<join_cross_keyword> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        state.join_type_override = relational::JoinType::Cross;
        state.join_type_override_set = true;
    }
};

template <>
struct select_action<kw_join> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.query == nullptr) {
            state.join_type_override_set = false;
            state.pending_join.reset();
            state.pending_join_predicate = nullptr;
            return;
        }

        relational::JoinClause::InputKind left_kind = relational::JoinClause::InputKind::Table;
        std::size_t left_index = 0U;

        if (state.last_join_index.has_value()) {
            left_kind = relational::JoinClause::InputKind::Join;
            left_index = *state.last_join_index;
        } else if (state.has_last_table) {
            left_kind = relational::JoinClause::InputKind::Table;
            left_index = state.last_table_index;
        } else {
            state.join_type_override_set = false;
            state.pending_join.reset();
            state.pending_join_predicate = nullptr;
            return;
        }

        relational::JoinType type = state.join_type_override_set ? state.join_type_override : relational::JoinType::Inner;
        state.join_type_override_set = false;

        SelectParseState::PendingJoin pending{};
        pending.type = type;
        pending.left_kind = left_kind;
        pending.left_index = left_index;
        pending.has_right = false;
        state.pending_join = pending;
        state.pending_join_predicate = nullptr;
    }
};

template <>
struct select_action<select_join_condition_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        auto* expression = pop_expression(state);
        state.pending_join_predicate = expression;
        state.expression_stack.clear();
    }
};

template <>
struct select_action<select_join_clause_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.query == nullptr || !state.pending_join.has_value()) {
            state.pending_join.reset();
            state.pending_join_predicate = nullptr;
            return;
        }

        auto pending = *state.pending_join;
        state.pending_join.reset();

        if (!pending.has_right) {
            state.pending_join_predicate = nullptr;
            return;
        }

        relational::JoinClause clause{};
        clause.type = pending.type;
        clause.left_kind = pending.left_kind;
        clause.left_index = pending.left_index;
        clause.right_index = pending.right_index;
        clause.predicate = state.pending_join_predicate;

        state.query->joins.push_back(std::move(clause));
        state.last_join_index = state.query->joins.size() - 1U;
        state.pending_join_predicate = nullptr;
    }
};

template <>
struct select_action<where_expression_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.query == nullptr) {
            state.expression_stack.clear();
            return;
        }

        state.query->where = pop_expression(state);
        state.expression_stack.clear();
    }
};

template <>
struct select_action<order_expression_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr || state.query == nullptr) {
            state.expression_stack.clear();
            return;
        }

        auto* expression = pop_expression(state);
        if (expression == nullptr) {
            state.expression_stack.clear();
            return;
        }

        auto& item = state.arena->make<relational::OrderByItem>();
        item.expression = expression;
        item.direction = relational::OrderByItem::Direction::Ascending;
        state.query->order_by.push_back(&item);
        state.current_order_item = &item;
        state.expression_stack.clear();
    }
};

template <>
struct select_action<group_expression_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr || state.query == nullptr) {
            state.expression_stack.clear();
            return;
        }

        auto* expression = pop_expression(state);
        if (expression == nullptr) {
            state.expression_stack.clear();
            return;
        }

        state.query->group_by.push_back(expression);
        state.expression_stack.clear();
    }
};

template <>
struct select_action<order_item_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        state.current_order_item = nullptr;
    }
};

template <>
struct select_action<order_desc_keyword> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.current_order_item != nullptr) {
            state.current_order_item->direction = relational::OrderByItem::Direction::Descending;
        }
    }
};

template <>
struct select_action<order_asc_keyword> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.current_order_item != nullptr) {
            state.current_order_item->direction = relational::OrderByItem::Direction::Ascending;
        }
    }
};

template <>
struct select_action<limit_row_expression_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.arena == nullptr || state.query == nullptr) {
            state.expression_stack.clear();
            return;
        }

        auto* expression = pop_expression(state);
        if (expression == nullptr) {
            state.expression_stack.clear();
            return;
        }

        if (state.limit_clause == nullptr) {
            state.limit_clause = &state.arena->make<relational::LimitClause>();
        }

        state.limit_clause->row_count = expression;
        state.query->limit = state.limit_clause;
        state.expression_stack.clear();
    }
};

template <>
struct select_action<limit_offset_expression_rule> {
    template <typename Input>
    static void apply(const Input&, SelectParseState& state)
    {
        if (state.limit_clause == nullptr) {
            state.expression_stack.clear();
            return;
        }

        state.limit_clause->offset = pop_expression(state);
        state.expression_stack.clear();
    }
};

StatementType classify_statement(std::string_view text)
{
    std::size_t offset = 0U;
    const auto first = next_token(text, offset);
    const auto second = next_token(text, offset);

    if (iequals(first, "CREATE")) {
        if (iequals(second, "DATABASE")) {
            return StatementType::CreateDatabase;
        }
        if (iequals(second, "SCHEMA")) {
            return StatementType::CreateSchema;
        }
        if (iequals(second, "TABLE")) {
            return StatementType::CreateTable;
        }
        if (iequals(second, "VIEW")) {
            return StatementType::CreateView;
        }
    }

    if (iequals(first, "DROP")) {
        if (iequals(second, "DATABASE")) {
            return StatementType::DropDatabase;
        }
        if (iequals(second, "SCHEMA")) {
            return StatementType::DropSchema;
        }
        if (iequals(second, "TABLE")) {
            return StatementType::DropTable;
        }
    }

    return StatementType::Unknown;
}

ScriptStatement parse_script_statement(std::string text)
{
    ScriptStatement statement{};
    statement.text = trim_copy(text);
    if (statement.text.empty()) {
        return statement;
    }

    while (!statement.text.empty()) {
        if (statement.text.rfind("--", 0) == 0U) {
            const auto newline = statement.text.find('\n');
            if (newline == std::string::npos) {
                statement.text.clear();
                break;
            }
            statement.text.erase(0, newline + 1U);
            statement.text = trim_copy(statement.text);
            continue;
        }

        if (statement.text.rfind("/*", 0) == 0U) {
            const auto terminator = statement.text.find("*/");
            if (terminator == std::string::npos) {
                statement.text.clear();
                break;
            }
            statement.text.erase(0, terminator + 2U);
            statement.text = trim_copy(statement.text);
            continue;
        }

        break;
    }

    if (statement.text.empty()) {
        return statement;
    }

    statement.type = classify_statement(statement.text);

    auto propagate = [&statement](auto&& parse_result) {
        statement.diagnostics = std::move(parse_result.diagnostics);
        if (parse_result.ast) {
            statement.success = true;
            using AstType = std::decay_t<decltype(*parse_result.ast)>;
            statement.ast.emplace<AstType>(std::move(*parse_result.ast));
        }
    };

    switch (statement.type) {
        case StatementType::CreateDatabase:
            propagate(parse_create_database(statement.text));
            break;
        case StatementType::DropDatabase:
            propagate(parse_drop_database(statement.text));
            break;
        case StatementType::CreateSchema:
            propagate(parse_create_schema(statement.text));
            break;
        case StatementType::DropSchema:
            propagate(parse_drop_schema(statement.text));
            break;
        case StatementType::CreateTable:
            propagate(parse_create_table(statement.text));
            break;
        case StatementType::DropTable:
            propagate(parse_drop_table(statement.text));
            break;
        case StatementType::CreateView:
            propagate(parse_create_view(statement.text));
            break;
        case StatementType::Unknown:
        default: {
            if (!statement.text.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Warning;
                diagnostic.message = "Unsupported DDL statement; only CREATE or DROP commands for DATABASE, "
                                     "SCHEMA, TABLE, and VIEW are recognised.";
                diagnostic.statement = statement.text;
                diagnostic.remediation_hints = {"Remove unsupported statements or extend the parser grammar."};
                statement.diagnostics.push_back(std::move(diagnostic));
            }
            break;
        }
    }

    return statement;
}

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
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match identifier grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
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
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match CREATE DATABASE grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
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
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match DROP DATABASE grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
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
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match CREATE SCHEMA grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
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
    DropSchemaParseState state{};

    try {
        const auto parsed = pegtl::parse<drop_schema_grammar, drop_schema_action>(in, statement, state);
        if (parsed) {
            append_duplicate_schema_diagnostics(statement, input, result.diagnostics);
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match DROP SCHEMA grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
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
    CreateTableParseState state{};

    try {
        const auto parsed = pegtl::parse<create_table_grammar, create_table_action>(in, statement, state);
        if (parsed) {
            append_duplicate_column_diagnostics(statement, input, result.diagnostics);
            result.ast = std::move(statement);
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match CREATE TABLE grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
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
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match CREATE VIEW grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
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
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match DROP TABLE grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
            result.diagnostics.push_back(std::move(diagnostic));
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error, input));
    }

    return result;
}

ScriptParseResult parse_ddl_script(std::string_view input)
{
    ScriptParseResult result{};
    std::size_t offset = 0U;

    while (offset < input.size()) {
        const auto terminator = find_statement_terminator(input, offset);
        const auto end = terminator == std::string_view::npos ? input.size() : terminator;
        const auto length = end >= offset ? end - offset : 0U;
        const auto raw = input.substr(offset, length);
        auto statement = parse_script_statement(std::string{raw});
        if (!statement.text.empty()) {
            result.statements.push_back(std::move(statement));
        }

        if (terminator == std::string_view::npos) {
            break;
        }

        offset = terminator + 1U;
    }

    return result;
}

SelectParseResult parse_select(std::string_view input)
{
    SelectParseResult result{};
    SelectParseState state{};
    state.arena = &result.arena;
    state.statement = &result.arena.make<relational::SelectStatement>();
    state.query = &result.arena.make<relational::QuerySpecification>();
    state.statement->query = state.query;

    pegtl::memory_input in(input, "select_statement");

    try {
        const auto parsed = pegtl::parse<select_statement_grammar, select_action>(in, state);
        if (parsed) {
            result.statement = state.statement;
        } else {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Warning;
            diagnostic.message = "input did not match SELECT grammar";
            diagnostic.line = 1U;
            diagnostic.column = 1U;
            diagnostic.statement = trim_copy(input);
            diagnostic.remediation_hints = {"Review the SQL syntax near the reported token."};
            result.diagnostics.push_back(std::move(diagnostic));
            result.statement = nullptr;
            result.arena.reset();
        }
    } catch (const pegtl::parse_error& error) {
        result.diagnostics.push_back(make_parse_error(error, input));
        result.statement = nullptr;
        result.arena.reset();
    }

    return result;
}

InsertParseResult parse_insert(std::string_view input)
{
    InsertParseResult result{};
    const auto normalized = strip_leading_comments(input);
    const auto statement_text = strip_trailing_semicolon(normalized);
    if (statement_text.empty()) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "INSERT statement is empty";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Provide an INSERT INTO statement with a VALUES clause."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    static const std::regex kInsertPattern(
        R"(^INSERT\s+INTO\s+([A-Za-z0-9_."]+)\s*\(([\s\S]+?)\)\s+VALUES\s+([\s\S]+)$)",
        std::regex::icase);

    std::smatch match;
    if (!std::regex_match(std::string{statement_text}, match, kInsertPattern)) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "INSERT syntax is not supported in this context";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Use INSERT INTO <schema.table> (<columns>) VALUES (...)."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    auto& statement = result.arena.make<relational::InsertStatement>();
    result.statement = &statement;

    auto& table_ref = result.arena.make<relational::TableReference>();
    table_ref.name = make_qualified_name(trim_copy(match[1].str()));
    statement.target = &table_ref;

    const auto columns_text = trim_copy(match[2].str());
    std::vector<std::string> column_names;
    if (!parse_identifier_list(columns_text, column_names)) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "INSERT column list is empty";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"List one or more columns inside parentheses after the table name."};
        result.diagnostics.push_back(std::move(diagnostic));
        result.statement = nullptr;
        result.arena.reset();
        return result;
    }

    statement.columns.reserve(column_names.size());
    for (auto& name : column_names) {
        relational::InsertColumn column{};
        column.name = make_identifier(name);
        statement.columns.push_back(std::move(column));
    }

    const auto values_text = trim_copy(match[3].str());
    std::string error;
    std::vector<std::string> hints;
    if (!populate_insert_values(values_text, result.arena, statement, error, hints)) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = std::move(error);
        diagnostic.statement = trim_copy(input);
        if (!hints.empty()) {
            diagnostic.remediation_hints = std::move(hints);
        }
        result.diagnostics.push_back(std::move(diagnostic));
        result.statement = nullptr;
        result.arena.reset();
        return result;
    }

    return result;
}

namespace {

relational::IdentifierExpression& make_identifier_expression(relational::AstArena& arena, std::string_view text)
{
    auto& expression = arena.make<relational::IdentifierExpression>();
    expression.name = make_qualified_name(text);
    return expression;
}

relational::LiteralExpression& make_numeric_literal_expression(relational::AstArena& arena, std::string token)
{
    auto& expression = arena.make<relational::LiteralExpression>();
    if (is_decimal_literal(token)) {
        expression.tag = relational::LiteralTag::Decimal;
    } else {
        expression.tag = relational::LiteralTag::Integer;
    }
    expression.text = std::move(token);
    return expression;
}

relational::LiteralExpression& make_string_literal_expression(relational::AstArena& arena, std::string text)
{
    auto& expression = arena.make<relational::LiteralExpression>();
    expression.tag = relational::LiteralTag::String;
    expression.text = std::move(text);
    return expression;
}

relational::LiteralExpression& make_boolean_literal_expression(relational::AstArena& arena, bool value)
{
    auto& expression = arena.make<relational::LiteralExpression>();
    expression.tag = relational::LiteralTag::Boolean;
    expression.boolean_value = value;
    expression.text = value ? "TRUE" : "FALSE";
    return expression;
}

relational::LiteralExpression& make_null_literal_expression(relational::AstArena& arena)
{
    auto& expression = arena.make<relational::LiteralExpression>();
    expression.tag = relational::LiteralTag::Null;
    expression.text.clear();
    return expression;
}

}  // namespace

UpdateParseResult parse_update(std::string_view input)
{
    UpdateParseResult result{};
    const auto normalized = strip_leading_comments(input);
    const auto statement_text = strip_trailing_semicolon(normalized);
    if (statement_text.empty()) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement is empty";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Provide an UPDATE <table> SET ... WHERE ... statement."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    auto rest = trim_copy(statement_text.substr(kUpdateKeyword.size()));
    if (rest.empty()) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement requires a target table";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Specify a table name immediately after UPDATE."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    std::size_t position = 0U;
    auto rest_view = std::string_view(rest);
    skip_whitespace(rest_view, position);
    const auto table_start = position;
    while (position < rest_view.size() && !std::isspace(static_cast<unsigned char>(rest_view[position]))) {
        ++position;
    }
    if (table_start == position) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement requires a target table";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Specify a table name immediately after UPDATE."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }
    const auto table_spec = trim_copy(rest_view.substr(table_start, position - table_start));
    skip_whitespace(rest_view, position);

    if (position + 3U > rest_view.size() || uppercase_copy(rest_view.substr(position, 3U)) != "SET") {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "UPDATE statement must include a SET clause";
        diagnostic.statement = trim_copy(input);
        diagnostic.remediation_hints = {"Add SET <column> = <expression> before the WHERE clause."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }
    position += 3U;
    skip_whitespace(rest_view, position);

    const auto where_offset = rest_view.substr(position).find(
}  // namespace bored::parser
