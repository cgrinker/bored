#include "bored/shell/shell_engine.hpp"

#include <CLI/CLI.hpp>
#include <replxx.hxx>

#include <chrono>
#include <cctype>
#include <exception>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace {

std::string trim(std::string_view text)
{
    std::size_t start = 0U;
    while (start < text.size() && std::isspace(static_cast<unsigned char>(text[start])) != 0) {
        ++start;
    }

    std::size_t end = text.size();
    while (end > start && std::isspace(static_cast<unsigned char>(text[end - 1U])) != 0) {
        --end;
    }

    return std::string{text.substr(start, end - start)};
}

void render_result(const bored::shell::CommandMetrics& metrics)
{
    const auto status = metrics.success ? "OK" : "ERROR";
    std::cout << status << ": " << metrics.summary;
    std::cout << " [" << std::fixed << std::setprecision(2) << metrics.duration_ms << " ms]";
    if (metrics.rows_touched != 0U || metrics.wal_bytes != 0U) {
        std::cout << " rows=" << metrics.rows_touched << " wal=" << metrics.wal_bytes;
    }
    std::cout << '\n';

    for (const auto& diagnostic : metrics.diagnostics) {
        std::cout << "  - " << diagnostic.message;
        if (!diagnostic.statement.empty()) {
            std::cout << " (statement: " << diagnostic.statement << ')';
        }
        std::cout << '\n';
        if (!diagnostic.remediation_hints.empty()) {
            for (const auto& hint : diagnostic.remediation_hints) {
                std::cout << "      hint: " << hint << '\n';
            }
        }
    }
}

bool command_complete(std::string_view text)
{
    for (std::size_t index = text.size(); index > 0U; --index) {
        const char ch = text[index - 1U];
        if (std::isspace(static_cast<unsigned char>(ch)) != 0) {
            continue;
        }
        if (ch == ';') {
            return true;
        }
        break;
    }
    return false;
}

int run_repl(bool quiet)
{
    replxx::Replxx repl;
    bored::shell::ShellEngine engine;

    if (!quiet) {
        std::cout << "bored shell — enter SQL statements terminated with ';' or type \\help.\n";
    }

    std::string buffer;
    while (true) {
        const char* line = repl.input(buffer.empty() ? "bored> " : "...> ");
        if (line == nullptr) {
            std::cout << '\n';
            break;
        }

        std::string_view view{line};
        const auto trimmed = trim(view);
        if (buffer.empty() && trimmed.rfind("\\", 0U) == 0U) {
            if (trimmed == "\\q" || trimmed == "\\quit") {
                break;
            }
            if (trimmed == "\\help") {
                std::cout << "Commands:\n";
                std::cout << "  SQL statements must end with ';'\n";
                std::cout << "  \\help      Show this message\n";
                std::cout << "  \\quit      Exit the shell\n";
                continue;
            }
            std::cout << "Unknown command: " << trimmed << '\n';
            continue;
        }

        if (trimmed.empty()) {
            continue;
        }

        repl.history_add(trimmed);
        buffer.append(line);
        buffer.push_back('\n');

        if (!command_complete(buffer)) {
            continue;
        }

        const auto result = engine.execute_sql(buffer);
        render_result(result);
        buffer.clear();
    }

    return 0;
}

int run_batch(const std::vector<std::string>& commands)
{
    bored::shell::ShellEngine engine;
    int exit_code = 0;
    for (const auto& command : commands) {
        const auto result = engine.execute_sql(command);
        render_result(result);
        if (!result.success) {
            exit_code = 1;
        }
    }
    return exit_code;
}

}  // namespace

int main(int argc, char** argv)
{
    CLI::App app{"Interactive SQL shell for the bored prototype."};

    bool quiet = false;
    std::vector<std::string> execute_commands;

    app.add_flag("-q,--quiet", quiet, "Suppress startup banner");
    app.add_option("-c,--command", execute_commands, "Execute the provided SQL command and exit")
        ->type_name("SQL")
        ->expected(1);

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return app.exit(error);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }

    if (!execute_commands.empty()) {
        return run_batch(execute_commands);
    }

    return run_repl(quiet);
}
