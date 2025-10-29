#include "bored/storage/index_comparator_registry.hpp"

#include <algorithm>
#include <array>
#include <compare>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <utility>

#include <catch2/catch_test_macros.hpp>

namespace {

template <typename Numeric>
std::array<std::byte, sizeof(Numeric)> to_bytes(Numeric value)
{
    std::array<std::byte, sizeof(Numeric)> buffer{};
    std::memcpy(buffer.data(), &value, sizeof(Numeric));
    return buffer;
}

}  // namespace

using bored::storage::find_index_comparator;
using bored::storage::index_comparator_names;
using bored::storage::register_index_comparator;
using bored::storage::IndexComparatorEntry;

TEST_CASE("Index comparator registry exposes built-ins")
{
    const auto names = index_comparator_names();
    CHECK(std::find(names.begin(), names.end(), "bytelex_ascending") != names.end());
    CHECK(std::find(names.begin(), names.end(), "utf8_ascending") != names.end());
    CHECK(std::find(names.begin(), names.end(), "int64_ascending") != names.end());
    CHECK(std::find(names.begin(), names.end(), "uint32_ascending") != names.end());
    CHECK(std::find(names.begin(), names.end(), "uint16_ascending") != names.end());
}

TEST_CASE("Numeric comparators order values as expected")
{
    SECTION("int64_ascending")
    {
        const auto* comparator = find_index_comparator("int64_ascending");
        REQUIRE(comparator != nullptr);
        const auto lhs = to_bytes<std::int64_t>(-5);
        const auto rhs = to_bytes<std::int64_t>(11);
        CHECK(comparator->compare(std::span<const std::byte>(lhs), std::span<const std::byte>(rhs)) == std::strong_ordering::less);
        CHECK(comparator->compare(std::span<const std::byte>(rhs), std::span<const std::byte>(lhs)) == std::strong_ordering::greater);
        CHECK(comparator->compare(std::span<const std::byte>(rhs), std::span<const std::byte>(rhs)) == std::strong_ordering::equal);
    }

    SECTION("uint32_ascending")
    {
        const auto* comparator = find_index_comparator("uint32_ascending");
        REQUIRE(comparator != nullptr);
        const auto lhs = to_bytes<std::uint32_t>(73U);
        const auto rhs = to_bytes<std::uint32_t>(99U);
        CHECK(comparator->compare(std::span<const std::byte>(lhs), std::span<const std::byte>(rhs)) == std::strong_ordering::less);
        CHECK(comparator->compare(std::span<const std::byte>(rhs), std::span<const std::byte>(lhs)) == std::strong_ordering::greater);
    }
}

TEST_CASE("Utf8 comparator compares lexicographically")
{
    const auto* comparator = find_index_comparator("utf8_ascending");
    REQUIRE(comparator != nullptr);
    const std::string lhs_text = "alpha";
    const std::string rhs_text = "beta";
    CHECK(comparator->compare(std::as_bytes(std::span(lhs_text)), std::as_bytes(std::span(rhs_text))) == std::strong_ordering::less);
    CHECK(comparator->compare(std::as_bytes(std::span(rhs_text)), std::as_bytes(std::span(lhs_text))) == std::strong_ordering::greater);
}

TEST_CASE("Bytelex comparator falls back to raw bytes")
{
    const auto* comparator = find_index_comparator("bytelex_ascending");
    REQUIRE(comparator != nullptr);
    const std::array<std::byte, 2> lhs{std::byte{0x01}, std::byte{0xFF}};
    const std::array<std::byte, 2> rhs{std::byte{0x02}, std::byte{0x00}};
    CHECK(comparator->compare(lhs, rhs) == std::strong_ordering::less);
}

std::strong_ordering always_equal(std::span<const std::byte>, std::span<const std::byte>) noexcept
{
    return std::strong_ordering::equal;
}

TEST_CASE("Index comparator registry allows custom registration")
{
    const std::string name = "custom_index_comparator";
    const auto* existing = find_index_comparator(name);
    if (existing == nullptr) {
        IndexComparatorEntry entry{};
        entry.name = name;
        entry.compare = always_equal;
        entry.fixed_length = 0U;
        REQUIRE(register_index_comparator(std::move(entry)));
    }

    const auto* comparator = find_index_comparator(name);
    REQUIRE(comparator != nullptr);
    CHECK(comparator->compare(std::span<const std::byte>(), std::span<const std::byte>()) == std::strong_ordering::equal);
    IndexComparatorEntry duplicate{};
    duplicate.name = name;
    duplicate.compare = always_equal;
    duplicate.fixed_length = 0U;
    CHECK_FALSE(register_index_comparator(std::move(duplicate)));
}

TEST_CASE("Unknown comparators return nullptr")
{
    CHECK(find_index_comparator("__does_not_exist__") == nullptr);
}
