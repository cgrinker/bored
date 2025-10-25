#include "bored/parser/grammar.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace bored::parser;

namespace {

namespace fs = std::filesystem;

std::string trim_copy(std::string_view view)
{
    const auto begin = view.find_first_not_of(" \t\r\n");
    if (begin == std::string_view::npos) {
        return {};
    }
    const auto end = view.find_last_not_of(" \t\r\n");
    return std::string(view.substr(begin, end - begin + 1));
}

fs::path samples_directory()
{
    return fs::path{__FILE__}.parent_path() / "parser_samples";
}

std::vector<std::pair<std::string, std::string>> load_valid_samples()
{
    const auto path = samples_directory() / "valid_identifiers.sql";
    std::ifstream stream(path);
    REQUIRE(stream.is_open());

    std::vector<std::pair<std::string, std::string>> samples{};
    std::string line;
    while (std::getline(stream, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        if (line.starts_with("--")) {
            continue;
        }

        const auto separator = line.find('|');
        std::string input;
        std::string expected;
        if (separator == std::string::npos) {
            input = line;
            expected = trim_copy(line);
        } else {
            input = line.substr(0, separator);
            expected = trim_copy(line.substr(separator + 1));
        }

        samples.emplace_back(std::move(input), std::move(expected));
    }

    return samples;
}

std::vector<std::string> load_invalid_samples()
{
    const auto path = samples_directory() / "invalid_identifiers.sql";
    std::ifstream stream(path);
    REQUIRE(stream.is_open());

    std::vector<std::string> samples{};
    std::string line;
    while (std::getline(stream, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        if (line.starts_with("--")) {
            continue;
        }
        samples.push_back(line);
    }
    return samples;
}

}  // namespace

TEST_CASE("parse_identifier accepts valid identifiers")
{
    const auto samples = load_valid_samples();
    REQUIRE_FALSE(samples.empty());

    for (const auto& [input, expected] : samples) {
        DYNAMIC_SECTION("input=" << input)
        {
            const auto result = parse_identifier(input);
            INFO("input: " << input);
            REQUIRE(result.success());
            REQUIRE(result.ast.has_value());
            CHECK(result.ast->value == expected);
            CHECK(result.diagnostics.empty());
        }
    }
}

TEST_CASE("parse_identifier rejects invalid identifiers")
{
    const auto samples = load_invalid_samples();
    REQUIRE_FALSE(samples.empty());

    for (const auto& input : samples) {
        DYNAMIC_SECTION("input=" << input)
        {
            const auto result = parse_identifier(input);
            INFO("input: " << input);
            CHECK_FALSE(result.success());
            REQUIRE_FALSE(result.diagnostics.empty());
            CHECK(result.diagnostics.front().severity == ParserSeverity::Error);
        }
    }
}
