#pragma once

#include "bored/storage/wal_reader.hpp"

#include <chrono>
#include <filesystem>
#include <system_error>
#include <vector>

namespace bored::storage {

struct WalRetentionConfig final {
    std::size_t retention_segments = 0U;
    std::chrono::hours retention_hours{0};
    std::filesystem::path archive_path{};
};

class WalRetentionManager final {
public:
    WalRetentionManager(std::filesystem::path wal_directory,
                        std::string file_prefix,
                        std::string file_extension);

    std::error_code apply(const WalRetentionConfig& config, std::uint64_t current_segment_id) const;

private:
    using SegmentList = std::vector<WalSegmentView>;

    std::error_code gather_segments(SegmentList& segments) const;
    std::error_code ensure_archive_directory(const std::filesystem::path& path) const;
    bool should_prune_by_time(const WalRetentionConfig& config, const std::filesystem::path& path) const;
    std::error_code prune_segment(const WalRetentionConfig& config, const WalSegmentView& segment) const;

    std::filesystem::path wal_directory_{};
    std::string file_prefix_{};
    std::string file_extension_{};
};

}  // namespace bored::storage
