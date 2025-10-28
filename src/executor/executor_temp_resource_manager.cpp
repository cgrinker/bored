#include "bored/executor/executor_temp_resource_manager.hpp"

#include <chrono>

namespace bored::executor {
namespace {

[[nodiscard]] std::string sanitise_tag(std::string_view tag)
{
    std::string cleaned{tag};
    if (cleaned.empty()) {
        cleaned = "spill";
    }
    for (auto& ch : cleaned) {
        if (ch == '/' || ch == '\\' || ch == ':') {
            ch = '_';
        }
    }
    return cleaned;
}

}  // namespace

ExecutorTempResourceManager::ExecutorTempResourceManager()
    : ExecutorTempResourceManager(Config{})
{
}

ExecutorTempResourceManager::ExecutorTempResourceManager(Config config)
{
    if (config.registry != nullptr) {
        registry_ = config.registry;
    } else {
        registry_ = &owned_registry_;
        owns_registry_ = true;
    }

    if (config.base_directory.empty()) {
        base_directory_ = make_default_base_directory();
    } else {
        std::error_code ec;
        auto canonical = std::filesystem::weakly_canonical(config.base_directory, ec);
        base_directory_ = ec ? config.base_directory : canonical;
    }
}

std::filesystem::path ExecutorTempResourceManager::base_directory() const
{
    std::lock_guard guard{mutex_};
    return base_directory_;
}

storage::TempResourceRegistry& ExecutorTempResourceManager::registry() noexcept
{
    return *registry_;
}

const storage::TempResourceRegistry& ExecutorTempResourceManager::registry() const noexcept
{
    return *registry_;
}

std::error_code ExecutorTempResourceManager::create_spill_directory(std::string_view tag,
                                                                    std::filesystem::path& out_directory)
{
    if (auto ec = ensure_base_directory(); ec) {
        return ec;
    }

    const auto sequence = sequence_.fetch_add(1U, std::memory_order_relaxed);
    auto name = sanitise_tag(tag);
    name.push_back('_');
    name.append(std::to_string(sequence));

    std::filesystem::path candidate;
    {
        std::lock_guard guard{mutex_};
        candidate = base_directory_ / name;
    }

    std::error_code ec;
    std::filesystem::create_directories(candidate, ec);
    if (ec) {
        return ec;
    }

    registry().register_directory(candidate);
    out_directory = candidate;
    return {};
}

std::error_code ExecutorTempResourceManager::track_spill_directory(const std::filesystem::path& path)
{
    if (path.empty()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
        return ec;
    }

    registry().register_directory(path);
    return {};
}

std::error_code ExecutorTempResourceManager::track_scratch_segment(const std::filesystem::path& path)
{
    if (path.empty()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto parent = path.parent_path();
    if (!parent.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            return ec;
        }
    }

    registry().register_file(path);
    return {};
}

std::error_code ExecutorTempResourceManager::purge(storage::TempResourcePurgeReason reason,
                                                   storage::TempResourcePurgeStats* stats) const
{
    return registry().purge(reason, stats);
}

std::filesystem::path ExecutorTempResourceManager::make_default_base_directory() const
{
    auto root = std::filesystem::temp_directory_path();
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return root / ("bored_executor_spill_" + std::to_string(now));
}

std::error_code ExecutorTempResourceManager::ensure_base_directory()
{
    std::filesystem::path directory;
    {
        std::lock_guard guard{mutex_};
        directory = base_directory_;
    }

    std::error_code ec;
    std::filesystem::create_directories(directory, ec);
    return ec;
}

}  // namespace bored::executor
