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

}  // namespace bored::parser
