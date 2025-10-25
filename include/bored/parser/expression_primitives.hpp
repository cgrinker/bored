#pragma once

#include <tao/pegtl.hpp>

namespace bored::parser::expr {

namespace pegtl = tao::pegtl;

struct optional_space : pegtl::star<pegtl::space> {
};

struct string_literal_char : pegtl::sor<pegtl::seq<pegtl::one<'\''>, pegtl::one<'\''>>, pegtl::not_one<'\''>> {
};

struct string_literal : pegtl::seq<pegtl::one<'\''>, pegtl::star<string_literal_char>, pegtl::one<'\''>> {
};

struct signed_integer : pegtl::seq<pegtl::opt<pegtl::one<'+', '-'>>, pegtl::plus<pegtl::digit>> {
};

struct fractional_part : pegtl::seq<pegtl::one<'.'>, pegtl::plus<pegtl::digit>> {
};

struct numeric_literal : pegtl::seq<pegtl::opt<pegtl::one<'+', '-'>>, pegtl::plus<pegtl::digit>, pegtl::opt<fractional_part>> {
};

}  // namespace bored::parser::expr
