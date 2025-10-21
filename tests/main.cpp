#include "bored/greeter.hpp"

#include <catch2/catch_test_macros.hpp>

TEST_CASE("greeting is friendly")
{
    REQUIRE(bored::greeting() == "Stay curious!");
}
