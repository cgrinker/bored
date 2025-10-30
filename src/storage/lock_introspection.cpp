#include "bored/storage/lock_introspection.hpp"

#include <mutex>
#include <utility>

namespace bored::storage {
namespace {

LockSnapshotSampler g_lock_sampler{};
std::mutex g_lock_sampler_mutex;

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

}  // namespace

void set_global_lock_snapshot_sampler(LockSnapshotSampler sampler) noexcept
{
    std::scoped_lock lock{g_lock_sampler_mutex};
    g_lock_sampler = std::move(sampler);
}

LockSnapshotSampler get_global_lock_snapshot_sampler() noexcept
{
    std::scoped_lock lock{g_lock_sampler_mutex};
    return g_lock_sampler;
}

LockDiagnosticsDocument collect_global_lock_diagnostics()
{
    auto sampler = get_global_lock_snapshot_sampler();
    if (!sampler) {
        return LockDiagnosticsDocument{};
    }

    LockDiagnosticsDocument document{};
    document.locks = sampler();
    return document;
}

std::string lock_diagnostics_to_json(const LockDiagnosticsDocument& document)
{
    std::string json;
    json.reserve(512U);
    json.push_back('{');

    json.append("\"schema_version\":");
    json.append(std::to_string(document.schema_version));
    json.push_back(',');

    json.append("\"locks\":[");
    for (std::size_t i = 0U; i < document.locks.size(); ++i) {
        if (i > 0U) {
            json.push_back(',');
        }
        const auto& lock = document.locks[i];
        json.push_back('{');
        bool first = true;
        auto append_number = [&](const char* name, std::uint64_t value) {
            if (!first) {
                json.push_back(',');
            }
            first = false;
            json.push_back('"');
            json.append(name);
            json.append("\":");
            json.append(std::to_string(value));
        };
        auto append_string = [&](const char* name, const std::string& value) {
            if (!first) {
                json.push_back(',');
            }
            first = false;
            json.push_back('"');
            json.append(name);
            json.append("\":");
            append_json_string(json, value);
        };

        append_number("page", lock.page_id);
        append_number("shared", lock.total_shared);
        append_number("exclusive_depth", lock.exclusive_depth);
        append_string("exclusive_owner", lock.exclusive_owner);

        json.append(",\"holders\":[");
        for (std::size_t idx = 0U; idx < lock.holders.size(); ++idx) {
            if (idx > 0U) {
                json.push_back(',');
            }
            const auto& holder = lock.holders[idx];
            json.push_back('{');
            bool holder_first = true;
            auto append_holder = [&](const char* name, const std::string& value) {
                if (!holder_first) {
                    json.push_back(',');
                }
                holder_first = false;
                json.push_back('"');
                json.append(name);
                json.append("\":");
                append_json_string(json, value);
            };
            auto append_holder_number = [&](const char* name, std::uint64_t value) {
                if (!holder_first) {
                    json.push_back(',');
                }
                holder_first = false;
                json.push_back('"');
                json.append(name);
                json.append("\":");
                json.append(std::to_string(value));
            };

            append_holder("thread_id", holder.thread_id);
            append_holder_number("shared", holder.shared);
            append_holder_number("exclusive", holder.exclusive);
            json.push_back('}');
        }
        json.push_back(']');
        json.push_back('}');
    }
    json.push_back(']');
    json.push_back('}');
    return json;
}

}  // namespace bored::storage
