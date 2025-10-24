#include "bored/ddl/ddl_validation.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace bored::ddl;

TEST_CASE("is_valid_identifier enforces naming rules")
{
    CHECK(is_valid_identifier("valid_name"));
    CHECK(is_valid_identifier("_underscore"));
    CHECK(is_valid_identifier("dollar$"));
    CHECK_FALSE(is_valid_identifier(""));
    CHECK_FALSE(is_valid_identifier("9startsWithDigit"));
    CHECK_FALSE(is_valid_identifier("has-hyphen"));
    CHECK_FALSE(is_valid_identifier("contains space"));
}

TEST_CASE("validate_identifier returns error codes")
{
    CHECK_FALSE(validate_identifier("table_one"));

    const auto error = validate_identifier("bad-name");
    REQUIRE(error);
    CHECK(error == make_error_code(DdlErrc::InvalidIdentifier));
}

TEST_CASE("ensure_exists and ensure_absent map to provided error codes")
{
    CHECK_FALSE(ensure_exists(true, DdlErrc::SchemaNotFound));
    CHECK(ensure_exists(false, DdlErrc::SchemaNotFound) == make_error_code(DdlErrc::SchemaNotFound));

    CHECK_FALSE(ensure_absent(false, DdlErrc::SchemaAlreadyExists));
    CHECK(ensure_absent(true, DdlErrc::SchemaAlreadyExists) == make_error_code(DdlErrc::SchemaAlreadyExists));
}
