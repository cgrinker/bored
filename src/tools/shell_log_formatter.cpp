#include "bored/tools/shell_log_formatter.hpp"

#include <chrono>
#include <iomanip>
#include <sstream>
#include <string_view>

namespace {

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

}  // namespace

namespace bored::tools {

std::string format_shell_command_log_json(const bored::shell::CommandMetrics& metrics)
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

}  // namespace bored::tools
