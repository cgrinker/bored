#include "bored/shell/shell_backend.hpp"
#include "bored/shell/shell_engine.hpp"
#include "bored/tools/shell_log_formatter.hpp"

#include <CLI/CLI.hpp>
#include <replxx.hxx>

#if defined(_WIN32)
#include <windows.h>
#include <DbgHelp.h>
#pragma comment(lib, "Dbghelp.lib")
#endif

#include <algorithm>
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

#if defined(_WIN32)
std::mutex& symbol_mutex()
{
    static std::mutex mutex;
    return mutex;
}

bool ensure_symbol_handler()
{
    static std::once_flag once_flag;
    static bool initialized = false;
    std::call_once(once_flag, []() {
        HANDLE process = GetCurrentProcess();
        const DWORD options = SymGetOptions() | SYMOPT_LOAD_LINES | SYMOPT_UNDNAME;
        SymSetOptions(options);
        if (SymInitialize(process, nullptr, TRUE) == TRUE) {
            initialized = true;
        } else {
            std::cerr << "[debug] SymInitialize failed error=" << GetLastError() << '\n';
        }
    });
    return initialized;
}

void log_stack_trace(const char* label)
{
    if (!ensure_symbol_handler()) {
        std::cerr << "[debug] " << label << " stack trace unavailable" << '\n';
        return;
    }

    constexpr ULONG max_frames = 32U;
    void* stack[max_frames] = {};
    const USHORT captured = CaptureStackBackTrace(1U, max_frames, stack, nullptr);
    if (captured == 0U) {
        std::cerr << "[debug] " << label << " stack trace empty" << '\n';
        return;
    }

    std::lock_guard<std::mutex> guard{symbol_mutex()};
    HANDLE process = GetCurrentProcess();
    std::cerr << "[debug] " << label << " stack trace (" << captured << " frames)" << '\n';

    for (USHORT index = 0U; index < captured; ++index) {
        const DWORD64 address = reinterpret_cast<DWORD64>(stack[index]);

        alignas(SYMBOL_INFO) char symbol_buffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME]{};
        auto* symbol = reinterpret_cast<PSYMBOL_INFO>(symbol_buffer);
        symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
        symbol->MaxNameLen = MAX_SYM_NAME;

        DWORD64 displacement = 0U;
        const bool has_symbol = SymFromAddr(process, address, &displacement, symbol) == TRUE;

        IMAGEHLP_LINE64 line_info{};
        line_info.SizeOfStruct = sizeof(IMAGEHLP_LINE64);
        DWORD line_displacement = 0U;
        const bool has_line = SymGetLineFromAddr64(process, address, &line_displacement, &line_info) == TRUE;

        IMAGEHLP_MODULE64 module_info{};
        module_info.SizeOfStruct = sizeof(IMAGEHLP_MODULE64);
        const bool has_module = SymGetModuleInfo64(process, address, &module_info) == TRUE;

        std::cerr << "    [" << index << "] 0x" << std::hex << address << std::dec;
        if (has_module) {
            std::cerr << " (" << module_info.ModuleName << " base=0x" << std::hex << module_info.BaseOfImage << std::dec << ')';
        }
        if (has_symbol) {
            std::cerr << " " << symbol->Name;
            if (displacement != 0U) {
                std::cerr << "+0x" << std::hex << displacement << std::dec;
            }
        }
        if (has_line) {
            std::cerr << " -- " << line_info.FileName << ':' << line_info.LineNumber;
        }
        std::cerr << '\n';
    }
}

LONG CALLBACK shell_vectored_exception_handler(PEXCEPTION_POINTERS info)
{
    if (info != nullptr && info->ExceptionRecord != nullptr) {
        const auto* record = info->ExceptionRecord;
    const bool non_continuable = (record->ExceptionFlags & EXCEPTION_NONCONTINUABLE) != 0;
    std::cerr << "[debug] vectored exception code=0x" << std::hex << record->ExceptionCode
          << " flags=0x" << record->ExceptionFlags << " address=" << record->ExceptionAddress
                  << " continuable=" << (non_continuable ? "no" : "yes") << std::dec << '\n';
        log_stack_trace("vectored");
    } else {
        std::cerr << "[debug] vectored exception (no details available)" << '\n';
    }
    return EXCEPTION_CONTINUE_SEARCH;
}

LONG CALLBACK shell_unhandled_exception_filter(EXCEPTION_POINTERS* info)
{
    if (info != nullptr && info->ExceptionRecord != nullptr) {
        const auto* record = info->ExceptionRecord;
        std::cerr << "[debug] unhandled exception code=0x" << std::hex << record->ExceptionCode
                  << " flags=0x" << record->ExceptionFlags << " address=" << record->ExceptionAddress
                  << " parameters=" << record->NumberParameters << std::dec << '\n';
        log_stack_trace("unhandled");
    } else {
        std::cerr << "[debug] unhandled exception (no details available)" << '\n';
    }
    return EXCEPTION_EXECUTE_HANDLER;
}
#endif

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

    std::cerr << "[debug] run_repl exiting with code=0\n";
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
    std::cerr << "[debug] run_batch exiting with code=" << exit_code << '\n';
    return exit_code;
}

}  // namespace

int main(int argc, char** argv)
{
#if defined(_WIN32)
    static auto vectored_handle = AddVectoredExceptionHandler(1U, shell_vectored_exception_handler);
    SetUnhandledExceptionFilter(shell_unhandled_exception_filter);
#endif

    CLI::App app{"Interactive SQL shell for the bored prototype."};

    bool quiet = false;
    std::vector<std::string> execute_commands;
    std::vector<std::string> script_files;
    std::string log_json_path;
    std::string data_directory;
    std::string wal_directory_override;
    std::size_t wal_retention_segments = 0U;
    int wal_retention_hours = 0;
    std::string wal_archive_directory;
    std::size_t io_threads = 0U;
    std::size_t io_queue_depth = 0U;
    std::string io_backend_name;
    bool io_disable_full_fsync = false;

    app.add_flag("-q,--quiet", quiet, "Suppress startup banner");
    app.add_option("-c,--command", execute_commands, "Execute the provided SQL command and exit")
        ->type_name("SQL")
        ->expected(1);
    app.add_option("-f,--file", script_files, "Execute SQL commands from the specified script file (use '-' for stdin)")
        ->type_name("PATH")
        ->expected(1);
    app.add_option("--log-json", log_json_path, "Write structured command logs as JSON Lines (use '-' for stdout)");
    app.add_option("--data-dir", data_directory, "Directory for persistent shell catalog storage")
        ->type_name("PATH");
    app.add_option("--wal-dir", wal_directory_override, "Directory for shell WAL files (defaults to <data-dir>/wal)")
        ->type_name("PATH");
    app.add_option("--wal-retention-segments", wal_retention_segments, "Maximum WAL segments to keep before pruning")
        ->check(CLI::NonNegativeNumber);
    app.add_option("--wal-retention-hours", wal_retention_hours, "Retain WAL segments newer than this many hours")
        ->check(CLI::NonNegativeNumber);
    app.add_option("--wal-archive-dir", wal_archive_directory, "Archive directory for retained WAL segments")
        ->type_name("PATH");
    app.add_option("--io-threads", io_threads, "Async IO worker thread count")
        ->check(CLI::PositiveNumber);
    app.add_option("--io-depth", io_queue_depth, "Async IO queue depth")
        ->check(CLI::PositiveNumber);
    app.add_option("--io-backend", io_backend_name, "Async IO backend (auto, thread-pool, windows-ioring, linux-iouring, mac-dispatch)");
    app.add_flag("--io-no-full-fsync", io_disable_full_fsync, "Disable fsync after WAL flush (may risk data loss)");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        const auto code = app.exit(error);
        std::cerr << "[debug] exiting main via CLI parse error path code=" << code << '\n';
        return code;
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        std::cerr << "[debug] exiting main due to exception code=1\n";
        return 1;
    }

    bored::shell::ShellBackend::Config backend_config;
    if (!data_directory.empty()) {
        backend_config.storage_directory = std::filesystem::path(data_directory);
    }
    if (!wal_directory_override.empty()) {
        backend_config.wal_directory = std::filesystem::path(wal_directory_override);
    }
    if (wal_retention_segments > 0U) {
        backend_config.wal_retention_segments = wal_retention_segments;
    }
    backend_config.wal_retention_hours = std::chrono::hours{static_cast<std::int64_t>(wal_retention_hours)};
    if (!wal_archive_directory.empty()) {
        backend_config.wal_archive_directory = std::filesystem::path(wal_archive_directory);
    }
    if (io_threads > 0U) {
        backend_config.io_worker_threads = io_threads;
    }
    if (io_queue_depth > 0U) {
        backend_config.io_queue_depth = io_queue_depth;
    }
    if (!io_backend_name.empty()) {
        std::string backend_text = io_backend_name;
        std::transform(backend_text.begin(), backend_text.end(), backend_text.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        if (backend_text == "auto") {
            backend_config.io_backend = bored::storage::AsyncIoBackend::Auto;
        } else if (backend_text == "thread-pool" || backend_text == "thread_pool" || backend_text == "threadpool") {
            backend_config.io_backend = bored::storage::AsyncIoBackend::ThreadPool;
        } else if (backend_text == "windows-ioring" || backend_text == "windows_ioring" || backend_text == "windowsioring") {
            backend_config.io_backend = bored::storage::AsyncIoBackend::WindowsIoRing;
        } else if (backend_text == "linux-iouring" || backend_text == "linux_iouring" || backend_text == "linuxiouring") {
            backend_config.io_backend = bored::storage::AsyncIoBackend::LinuxIoUring;
        } else if (backend_text == "mac-dispatch" || backend_text == "mac_dispatch" || backend_text == "macdispatch") {
            backend_config.io_backend = bored::storage::AsyncIoBackend::MacDispatch;
        } else {
            std::cerr << "error: unknown async IO backend '" << io_backend_name << "'" << '\n';
            std::cerr << "[debug] exiting main due to invalid io backend code=1\n";
            return 1;
        }
    }
    if (io_disable_full_fsync) {
        backend_config.io_use_full_fsync = false;
    }

    bored::shell::ShellBackend backend{backend_config};
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
                std::cerr << "[debug] exiting main due to log open failure code=1\n";
                return 1;
            }
            log_stream = file.get();
            log_file = std::move(file);
        }

        if (log_stream != nullptr) {
            config.command_logger = [log_stream, &log_mutex](const bored::shell::CommandMetrics& metrics) {
                const auto line = bored::tools::format_shell_command_log_json(metrics);
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
                std::cerr << "[debug] exiting main due to repeated stdin script code=1\n";
                return 1;
            }
            stdin_consumed = true;
            input = &std::cin;
        } else {
            script_stream.open(script_path);
            if (!script_stream.is_open()) {
                std::cerr << "error: failed to open script file '" << script_path << "'" << '\n';
                std::cerr << "[debug] exiting main due to script open failure code=1\n";
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
            std::cerr << "[debug] exiting main due to script load failure code=1\n";
            return 1;
        }
    }

    commands_to_run.insert(commands_to_run.end(), execute_commands.begin(), execute_commands.end());

    if (!commands_to_run.empty()) {
        const auto code = run_batch(commands_to_run, config);
        std::cerr << "[debug] exiting main via run_batch code=" << code << '\n';
        return code;
    }

    if (!script_files.empty()) {
        std::cerr << "[debug] exiting main after scripts with no commands code=0\n";
        return 0;
    }

    const auto repl_code = run_repl(quiet, config);
    std::cerr << "[debug] exiting main via run_repl code=" << repl_code << '\n';
    return repl_code;
}
