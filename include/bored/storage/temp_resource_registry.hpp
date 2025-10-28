#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <system_error>
#include <vector>

namespace bored::storage {

enum class TempResourcePurgeReason : std::uint8_t {
    Checkpoint,
    Recovery
};

struct TempResourcePurgeStats final {
    std::uint64_t checked_entries = 0U;
    std::uint64_t purged_entries = 0U;
    std::uint64_t errors = 0U;
};

class TempResourceRegistry final {
public:
    TempResourceRegistry() = default;
    TempResourceRegistry(const TempResourceRegistry&) = delete;
    TempResourceRegistry& operator=(const TempResourceRegistry&) = delete;

    void register_directory(const std::filesystem::path& path);
    void register_file(const std::filesystem::path& path);
    void clear();

    [[nodiscard]] bool empty() const noexcept;

    [[nodiscard]] std::error_code purge(TempResourcePurgeReason reason,
                                        TempResourcePurgeStats* stats = nullptr) const noexcept;

private:
    enum class EntryType : std::uint8_t {
        Directory,
        File
    };

    struct Entry final {
        std::filesystem::path path{};
        EntryType type = EntryType::File;
    };

    void add_entry(Entry entry);

    [[nodiscard]] std::error_code purge_directory(const std::filesystem::path& path,
                                                  std::uint64_t& purged) const;
    [[nodiscard]] std::error_code purge_file(const std::filesystem::path& path,
                                             std::uint64_t& purged) const;

    mutable std::mutex mutex_{};
    std::vector<Entry> entries_{};
};

}  // namespace bored::storage
