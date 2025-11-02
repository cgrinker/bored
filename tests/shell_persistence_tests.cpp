#include "bored/shell/shell_backend.hpp"
#include "bored/shell/shell_engine.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <string>
#include <system_error>

namespace {

std::filesystem::path make_unique_shell_path()
{
    const auto base = std::filesystem::temp_directory_path();
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return base / ("bored_shell_persistence_" + std::to_string(stamp));
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

}  // namespace

TEST_CASE("Shell backend persists catalog across restarts")
{
    TempShellDirectory temp_dir;

    bored::shell::ShellBackend::Config config;
    config.storage_directory = temp_dir.path;
    config.wal_directory = temp_dir.path / "wal";

    {
        bored::shell::ShellBackend backend{config};
        auto engine_config = backend.make_config();
        bored::shell::ShellEngine engine{engine_config};

        auto create = engine.execute_sql("CREATE TABLE widgets (id BIGINT, counter BIGINT, name TEXT);");
        REQUIRE(create.success);

        auto insert = engine.execute_sql("INSERT INTO widgets (id, counter, name) VALUES (1, 7, 'persisted');");
        REQUIRE(insert.success);
        CHECK(insert.rows_touched == 1U);
        CHECK(insert.wal_bytes > 0U);
    }

    {
        bored::shell::ShellBackend backend{config};
        auto engine_config = backend.make_config();
        bored::shell::ShellEngine engine{engine_config};

        auto select = engine.execute_sql("SELECT id, counter, name FROM widgets;");
        REQUIRE(select.success);
        CHECK(select.rows_touched >= 1U);

        bool found_row = false;
        for (const auto& line : select.detail_lines) {
            if (line.find("persisted") != std::string::npos && line.find("1") != std::string::npos &&
                line.find("7") != std::string::npos) {
                found_row = true;
                break;
            }
        }

        CHECK(found_row);
    }
}
