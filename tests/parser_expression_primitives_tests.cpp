#include "bored/parser/expression_primitives.hpp"

#include <catch2/catch_test_macros.hpp>
#include <tao/pegtl.hpp>

using namespace bored::parser;

namespace {

template <typename Rule>
void expect_parse_success(std::string_view input)
{
    tao::pegtl::memory_input in(input, "expression_rule_success");
    using complete_rule = tao::pegtl::seq<Rule, tao::pegtl::eof>;
    CHECK(tao::pegtl::parse<complete_rule>(in));
}

template <typename Rule>
void expect_parse_failure(std::string_view input)
{
    tao::pegtl::memory_input in(input, "expression_rule_failure");
    CAPTURE(input);
    using complete_rule = tao::pegtl::seq<Rule, tao::pegtl::eof>;
    CHECK_FALSE(tao::pegtl::parse<complete_rule>(in));
}

}  // namespace

TEST_CASE("string_literal accepts escaped quotes")
{
    expect_parse_success<expr::string_literal>("'can''t stop'");
    expect_parse_success<expr::string_literal>("''");
}

TEST_CASE("string_literal rejects unterminated input")
{
    expect_parse_failure<expr::string_literal>("'oops");
}

TEST_CASE("numeric_literal accepts signed integers and decimals")
{
    expect_parse_success<expr::numeric_literal>("42");
    expect_parse_success<expr::numeric_literal>("-17");
    expect_parse_success<expr::numeric_literal>("+3.1415");
    expect_parse_success<expr::numeric_literal>("0.25");
}

TEST_CASE("numeric_literal rejects invalid formats")
{
    expect_parse_failure<expr::numeric_literal>("--1");
    expect_parse_failure<expr::numeric_literal>("12.");
    expect_parse_failure<expr::numeric_literal>(".5");
}
