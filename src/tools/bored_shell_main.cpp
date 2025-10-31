#include "bored/shell/shell_backend.hpp"
#include "bored/shell/shell_engine.hpp"

#include <CLI/CLI.hpp>
#include <replxx.hxx>

#include <chrono>
#include <cctype>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
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

std::filesystem::path history_path()
{
    const char* home = std::getenv("HOME");
    if (home == nullptr || home[0] == '\0') {
        return {};
    }

    std::filesystem::path path{home};
    path /= ".bored_shell_history";
    return path;
}

void append_json_string(std::string& out, const std::string& text)
{
    out.push_back('"');
    for (unsigned char ch : text) {
        switch (ch) {
        case '"':
            out.append("\\\"");
            break;
        case '\\':
            out.append("\\\\");
            break;
        case '\b':
            out.append("\\b");
            break;
        case '\f':
            out.append("\\f");
            break;
        case '\n':
            out.append("\\n");
            break;
        case '\r':
            out.append("\\r");
            break;
        case '\t':
            out.append("\\t");
            break;
        default:
            if (ch < 0x20U) {
                constexpr char kHex[] = "0123456789ABCDEF";
                out.append("\\u00");
                out.push_back(kHex[(ch >> 4U) & 0x0F]);
                out.push_back(kHex[ch & 0x0F]);
            } else {
                out.push_back(static_cast<char>(ch));
            }
            break;
        }
    }
    out.push_back('"');
}

[[nodiscard]] std::string parser_severity_to_string(bored::parser::ParserSeverity severity)
{
    switch (severity) {
    case bored::parser::ParserSeverity::Info:
        return "info";
    case bored::parser::ParserSeverity::Warning:
        return "warning";
    case bored::parser::ParserSeverity::Error:
    default:
        return "error";
    }
}

[[nodiscard]] std::string format_timestamp_iso(std::chrono::system_clock::time_point tp)
{
    if (tp.time_since_epoch().count() == 0) {
        return {};
    }

    const auto time_value = std::chrono::system_clock::to_time_t(tp);
    std::tm buffer{};
#if defined(_WIN32)
    gmtime_s(&buffer, &time_value);
#else
    gmtime_r(&time_value, &buffer);
#endif

    std::ostringstream stream;
    stream << std::put_time(&buffer, "%Y-%m-%dT%H:%M:%S");
    const auto fractional = tp - std::chrono::system_clock::from_time_t(time_value);
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(fractional).count();
    stream << '.' << std::setw(6) << std::setfill('0') << micros << 'Z';
    return stream.str();
}

[[nodiscard]] std::string format_command_log_json(const bored::shell::CommandMetrics& metrics)
{
    std::string json;
    json.reserve(512U);
    json.push_back('{');
    bool first = true;

    auto append_field = [&](const char* name) {
        if (!first) {
            json.push_back(',');
        }
        first = false;
        json.push_back('"');
        json.append(name);
    json.push_back('"');
    json.push_back(':');
    };

    auto append_string_field = [&](const char* name, const std::string& value) {
        append_field(name);
        append_json_string(json, value);
    };

    auto append_number_field = [&](const char* name, auto value) {
        append_field(name);
        json.append(std::to_string(value));
    };

    auto append_bool_field = [&](const char* name, bool value) {
        append_field(name);
        json.append(value ? "true" : "false");
    };

    append_string_field("correlation_id", metrics.correlation_id);
    append_string_field("category", metrics.command_category);
    append_string_field("sql", metrics.command_text);
    append_string_field("summary", metrics.summary);
    append_bool_field("success", metrics.success);
    append_number_field("duration_ms", metrics.duration_ms);
    append_number_field("rows_touched", metrics.rows_touched);
    append_number_field("wal_bytes", metrics.wal_bytes);

    const auto started = format_timestamp_iso(metrics.started_at);
    append_field("started_at");
    if (started.empty()) {
        json.append("null");
    } else {
        append_json_string(json, started);
    }

    const auto finished = format_timestamp_iso(metrics.finished_at);
    append_field("finished_at");
    if (finished.empty()) {
        json.append("null");
    } else {
        append_json_string(json, finished);
    }

    append_field("detail_lines");
    json.push_back('[');
    for (std::size_t i = 0; i < metrics.detail_lines.size(); ++i) {
        if (i > 0U) {
            json.push_back(',');
        }
        append_json_string(json, metrics.detail_lines[i]);
    }
    json.push_back(']');

    append_field("diagnostics");
    json.push_back('[');
    for (std::size_t i = 0; i < metrics.diagnostics.size(); ++i) {
        if (i > 0U) {
            json.push_back(',');
        }
        const auto& diagnostic = metrics.diagnostics[i];
        json.push_back('{');
        bool diag_first = true;
        auto append_diag_field = [&](const char* name) {
            if (!diag_first) {
                json.push_back(',');
            }
            diag_first = false;
            json.push_back('"');
            json.append(name);
            json.push_back('"');
            json.push_back(':');
        };

        append_diag_field("severity");
        append_json_string(json, parser_severity_to_string(diagnostic.severity));
        append_diag_field("message");
        append_json_string(json, diagnostic.message);
        append_diag_field("line");
        json.append(std::to_string(diagnostic.line));
        append_diag_field("column");
        json.append(std::to_string(diagnostic.column));
        append_diag_field("statement");
        append_json_string(json, diagnostic.statement);
        append_diag_field("remediation_hints");
        json.push_back('[');
        for (std::size_t hint_index = 0; hint_index < diagnostic.remediation_hints.size(); ++hint_index) {
            if (hint_index > 0U) {
                json.push_back(',');
            }
            append_json_string(json, diagnostic.remediation_hints[hint_index]);
        }
        json.push_back(']');
        json.push_back('}');
    }
    json.push_back(']');

    json.push_back('}');
    return json;
}

void render_result(const bored::shell::CommandMetrics& metrics)
{
    const auto status = metrics.success ? "OK" : "ERROR";
    std::cout << status << ": " << metrics.summary;
    if (!metrics.correlation_id.empty()) {
        std::cout << " [" << metrics.correlation_id << ']';
    }
    std::cout << " [" << std::fixed << std::setprecision(2) << metrics.duration_ms << " ms]";
    if (metrics.rows_touched != 0U || metrics.wal_bytes != 0U) {
        std::cout << " rows=" << metrics.rows_touched << " wal=" << metrics.wal_bytes;
    }
    std::cout << '\n';

    for (const auto& line : metrics.detail_lines) {
        std::cout << "    " << line << '\n';
    }

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
    std::int32_t paren_depth = 0;
    bool in_single_quote = false;
    bool in_double_quote = false;
    bool in_line_comment = false;
    bool in_block_comment = false;

    for (std::size_t index = 0U; index < text.size(); ++index) {
        const char ch = text[index];
        const char next = (index + 1U < text.size()) ? text[index + 1U] : '\0';

        if (in_line_comment) {
            if (ch == '\n') {
                in_line_comment = false;
            }
            continue;
        }

        if (in_block_comment) {
            if (ch == '*' && next == '/') {
                in_block_comment = false;
                ++index;
            }
            continue;
        }

        if (!in_single_quote && !in_double_quote) {
            if (ch == '-' && next == '-') {
                in_line_comment = true;
                ++index;
                continue;
            }
            if (ch == '/' && next == '*') {
                in_block_comment = true;
                ++index;
                continue;
            }
        }

        if (ch == '\'' && !in_double_quote) {
            if (in_single_quote && next == '\'') {
                ++index;
            } else {
                in_single_quote = !in_single_quote;
            }
            continue;
        }

        if (ch == '"' && !in_single_quote) {
            if (in_double_quote && next == '"') {
                ++index;
            } else {
                in_double_quote = !in_double_quote;
            }
            continue;
        }

        if (in_single_quote || in_double_quote) {
            continue;
        }

        if (ch == '(') {
            ++paren_depth;
            continue;
        }

        if (ch == ')' && paren_depth > 0) {
            --paren_depth;
            continue;
        }

        if (ch == ';' && paren_depth == 0) {
            bool trailing_only_whitespace = true;
            for (std::size_t tail = index + 1U; tail < text.size(); ++tail) {
                const char remainder = text[tail];
                if (std::isspace(static_cast<unsigned char>(remainder)) != 0) {
                    continue;
                }
                if (remainder == '-' && tail + 1U < text.size() && text[tail + 1U] == '-') {
                    tail += 2U;
                    while (tail < text.size() && text[tail] != '\n') {
                        ++tail;
                    }
                    if (tail == text.size()) {
                        break;
                    }
                    continue;
                }
                if (remainder == '/' && tail + 1U < text.size() && text[tail + 1U] == '*') {
                    tail += 2U;
                    while (tail + 1U < text.size() && !(text[tail] == '*' && text[tail + 1U] == '/')) {
                        ++tail;
                    }
                    if (tail + 1U < text.size()) {
                        ++tail;
                    }
                    continue;
                }
                trailing_only_whitespace = false;
                break;
            }
            if (trailing_only_whitespace) {
                return true;
            }
        }
    }

    return false;
}

bool is_comment_only(std::string_view text)
{
    if (text.empty()) {
        return true;
    }

    if (text.rfind("--", 0U) == 0U) {
        return true;
    }

    if (text.rfind("/*", 0U) == 0U) {
        return text.find("*/") != std::string_view::npos;
    }

    return false;
}

bool load_script_commands(std::istream& input, std::vector<std::string>& commands, std::string& error_message)
{
    error_message.clear();

    std::string buffer;
    std::string line;

    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        buffer.append(line);
        buffer.push_back('\n');

        if (!command_complete(buffer)) {
            continue;
        }

        const auto statement = trim(buffer);
        if (!statement.empty()) {
            commands.push_back(statement);
        }
        buffer.clear();
    }

    if (input.bad()) {
        error_message = "I/O error while reading script";
        return false;
    }

    if (input.fail() && !input.eof()) {
        error_message = "Failed to read script to completion";
        return false;
    }

    const auto trailing = trim(buffer);
    if (!trailing.empty()) {
        if (!is_comment_only(trailing)) {
            commands.push_back(trailing);
        }
    }

    return true;
}

bool run_script_stream(bored::shell::ShellEngine& engine, std::istream& stream, const std::string& source)
{
    std::vector<std::string> statements;
    statements.reserve(16U);

    std::string error;
    if (!load_script_commands(stream, statements, error)) {
        std::cerr << "error: " << error;
        if (!source.empty()) {
            std::cerr << " ('" << source << "')";
        }
        std::cerr << '\n';
        return false;
    }

    bool all_success = true;
    for (const auto& statement : statements) {
        const auto result = engine.execute_sql(statement);
        render_result(result);
        if (!result.success) {
            all_success = false;
        }
    }

    return all_success;
}

int run_repl(bool quiet, const bored::shell::ShellEngine::Config& config)
{
    replxx::Replxx repl;
    bored::shell::ShellEngine engine{config};

    const auto history = history_path();
    if (!history.empty()) {
        (void)std::filesystem::create_directories(history.parent_path());
        (void)repl.history_load(history.string());
    }

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
            const auto result = engine.execute_sql(std::string(trimmed));
            render_result(result);
            continue;
        }

        if (buffer.empty() && trimmed.rfind("@", 0U) == 0U) {
            const auto script_spec = trim(trimmed.substr(1U));
            if (script_spec.empty()) {
                std::cerr << "error: script path is required after '@'" << '\n';
                continue;
            }

            if (script_spec == "-") {
                std::cerr << "error: reading scripts from stdin is not supported inside the interactive shell" << '\n';
                continue;
            }

            std::ifstream script_file{script_spec};
            if (!script_file.is_open()) {
                std::cerr << "error: failed to open script file '" << script_spec << "'" << '\n';
                continue;
            }

            if (!run_script_stream(engine, script_file, script_spec)) {
                std::cerr << "error: script '" << script_spec << "' completed with errors" << '\n';
            }
            continue;
        }

        if (trimmed.empty()) {
            if (!buffer.empty()) {
                buffer.append(line);
                buffer.push_back('\n');
            }
            continue;
        }

        buffer.append(line);
        buffer.push_back('\n');

        if (!command_complete(buffer)) {
            continue;
        }

        const auto statement = trim(buffer);
        if (!statement.empty()) {
            repl.history_add(statement);
        }

        const auto result = engine.execute_sql(buffer);
        render_result(result);
        if (!history.empty()) {
            (void)repl.history_save(history.string());
        }
        buffer.clear();
    }

    return 0;
}

int run_batch(const std::vector<std::string>& commands, const bored::shell::ShellEngine::Config& config)
{
    bored::shell::ShellEngine engine{config};
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
    std::vector<std::string> script_files;
    std::string log_json_path;

    app.add_flag("-q,--quiet", quiet, "Suppress startup banner");
    app.add_option("-c,--command", execute_commands, "Execute the provided SQL command and exit")
        ->type_name("SQL")
        ->expected(1);
    app.add_option("-f,--file", script_files, "Execute SQL commands from the specified script file (use '-' for stdin)")
        ->type_name("PATH")
        ->expected(1);
    app.add_option("--log-json", log_json_path, "Write structured command logs as JSON Lines (use '-' for stdout)");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return app.exit(error);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }

    bored::shell::ShellBackend backend;
    auto config = backend.make_config();
    std::unique_ptr<std::ofstream> log_file;
    std::ostream* log_stream = nullptr;
    std::mutex log_mutex;

    if (!log_json_path.empty()) {
        if (log_json_path == "-") {
            log_stream = &std::cout;
        } else {
            auto file = std::make_unique<std::ofstream>(log_json_path, std::ios::out | std::ios::app);
            if (!file->is_open()) {
                std::cerr << "error: failed to open log file '" << log_json_path << "'" << '\n';
                return 1;
            }
            log_stream = file.get();
            log_file = std::move(file);
        }

        if (log_stream != nullptr) {
            config.command_logger = [log_stream, &log_mutex](const bored::shell::CommandMetrics& metrics) {
                const auto line = format_command_log_json(metrics);
                std::lock_guard<std::mutex> guard{log_mutex};
                (*log_stream) << line << '\n';
                log_stream->flush();
            };
        }
    }

    std::vector<std::string> commands_to_run;
    commands_to_run.reserve(execute_commands.size());

    bool stdin_consumed = false;
    for (const auto& script_path : script_files) {
        std::istream* input = nullptr;
        std::ifstream script_stream;
        if (script_path == "-") {
            if (stdin_consumed) {
                std::cerr << "error: stdin script '-' specified more than once" << '\n';
                return 1;
            }
            stdin_consumed = true;
            input = &std::cin;
        } else {
            script_stream.open(script_path);
            if (!script_stream.is_open()) {
                std::cerr << "error: failed to open script file '" << script_path << "'" << '\n';
                return 1;
            }
            input = &script_stream;
        }

        std::string error;
        if (!load_script_commands(*input, commands_to_run, error)) {
            std::cerr << "error: " << error;
            if (script_path == "-") {
                std::cerr << " ('<stdin>')";
            } else {
                std::cerr << " ('" << script_path << "')";
            }
            std::cerr << '\n';
            return 1;
        }
    }

    commands_to_run.insert(commands_to_run.end(), execute_commands.begin(), execute_commands.end());

    if (!commands_to_run.empty()) {
        return run_batch(commands_to_run, config);
    }

    if (!script_files.empty()) {
        return 0;
    }

    return run_repl(quiet, config);
}
