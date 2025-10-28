#pragma once

#include "bored/storage/temp_resource_registry.hpp"

#include <atomic>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>

namespace bored::executor {

class ExecutorTempResourceManager final {
public:
    struct Config final {
        std::filesystem::path base_directory{};
        storage::TempResourceRegistry* registry = nullptr;
    };

    ExecutorTempResourceManager();
    explicit ExecutorTempResourceManager(Config config);

    [[nodiscard]] std::filesystem::path base_directory() const;
    [[nodiscard]] storage::TempResourceRegistry& registry() noexcept;
    [[nodiscard]] const storage::TempResourceRegistry& registry() const noexcept;

    [[nodiscard]] std::error_code create_spill_directory(std::string_view tag,
                                                         std::filesystem::path& out_directory);
    [[nodiscard]] std::error_code track_spill_directory(const std::filesystem::path& path);
    [[nodiscard]] std::error_code track_scratch_segment(const std::filesystem::path& path);

    [[nodiscard]] std::error_code purge(storage::TempResourcePurgeReason reason,
                                        storage::TempResourcePurgeStats* stats = nullptr) const;

private:
    [[nodiscard]] std::filesystem::path make_default_base_directory() const;
    [[nodiscard]] std::error_code ensure_base_directory();

    storage::TempResourceRegistry* registry_ = nullptr;
    bool owns_registry_ = false;
    storage::TempResourceRegistry owned_registry_{};
    std::filesystem::path base_directory_{};
    std::atomic<std::uint64_t> sequence_{0U};
    mutable std::mutex mutex_{};
};

}  // namespace bored::executor
