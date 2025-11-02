#include "bored/tools/shell_log_formatter.hpp"

#include "bored/shell/shell_backend.hpp"
#include "bored/shell/shell_engine.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cctype>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>

namespace {

std::filesystem::path make_unique_shell_path()
{
    const auto base = std::filesystem::temp_directory_path();
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return base / ("bored_shell_log_" + std::to_string(stamp));
}

struct TempShellDirectory final {
    TempShellDirectory()
        : path{make_unique_shell_path()}
    {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    ~TempShellDirectory()
    {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    std::filesystem::path path;
};

struct ShellLogHarness final {
    ShellLogHarness()
        : temp{}
        , backend{make_backend_config(temp.path)}
        , engine{backend.make_config()}
    {
        auto create = engine.execute_sql("CREATE TABLE metrics (id BIGINT, counter BIGINT, name TEXT);");
        REQUIRE(create.success);

        auto insert = engine.execute_sql("INSERT INTO metrics (id, counter, name) VALUES (1, 4, 'alpha');");
        REQUIRE(insert.success);
        CAPTURE(insert.summary);
        CAPTURE(insert.rows_touched);
        CAPTURE(insert.wal_bytes);
        CHECK(insert.rows_touched == 1U);
        CHECK(insert.wal_bytes > 0U);
    }

    static bored::shell::ShellBackend::Config make_backend_config(const std::filesystem::path& data_directory)
    {
        bored::shell::ShellBackend::Config config;
        config.storage_directory = data_directory;
        config.wal_directory = data_directory / "wal";
        return config;
    }

    TempShellDirectory temp;
    bored::shell::ShellBackend backend{};
    bored::shell::ShellEngine engine;
};

std::uint64_t extract_json_number(const std::string& json, std::string_view field)
{
    const auto pos = json.find(field);
    REQUIRE(pos != std::string::npos);
    auto value_pos = pos + field.size();
    auto end = value_pos;
    while (end < json.size() && std::isdigit(static_cast<unsigned char>(json[end])) != 0) {
        ++end;
    }
    REQUIRE(end > value_pos);
    return std::stoull(json.substr(value_pos, end - value_pos));
}

}  // namespace

TEST_CASE("Shell log JSON encodes DML telemetry counters")
{
    ShellLogHarness harness;

    const auto metrics = harness.engine.execute_sql(
        "INSERT INTO metrics (id, counter, name) VALUES (2, 7, 'beta');");
    REQUIRE(metrics.success);
    CAPTURE(metrics.summary);
    CAPTURE(metrics.rows_touched);
    CAPTURE(metrics.wal_bytes);
    REQUIRE(metrics.rows_touched == 1U);
    REQUIRE(metrics.wal_bytes > 0U);

    const auto json = bored::tools::format_shell_command_log_json(metrics);

    CHECK(json.find("\"summary\":\"Inserted 1 row.\"") != std::string::npos);
    CHECK(json.find("\"success\":true") != std::string::npos);

    const auto rows = extract_json_number(json, "\"rows_touched\":");
    CHECK(rows == metrics.rows_touched);

    const auto wal = extract_json_number(json, "\"wal_bytes\":");
    CHECK(wal == metrics.wal_bytes);
    CHECK(wal > 0U);
}
