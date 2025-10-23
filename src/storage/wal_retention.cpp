#include "bored/storage/wal_retention.hpp"

#include "bored/storage/wal_reader.hpp"

#include <algorithm>
#include <unordered_set>
#include <utility>

namespace bored::storage {

namespace {

constexpr bool has_retention_policy(const WalRetentionConfig& config)
{
    return config.retention_segments > 0U
        || config.retention_hours.count() > 0
        || !config.archive_path.empty();
}

}  // namespace

WalRetentionManager::WalRetentionManager(std::filesystem::path wal_directory,
                                         std::string file_prefix,
                                         std::string file_extension)
    : wal_directory_{std::move(wal_directory)}
    , file_prefix_{std::move(file_prefix)}
    , file_extension_{std::move(file_extension)}
{
}

std::error_code WalRetentionManager::gather_segments(SegmentList& segments) const
{
    WalReader reader{wal_directory_, file_prefix_, file_extension_};
    return reader.enumerate_segments(segments);
}

std::error_code WalRetentionManager::ensure_archive_directory(const std::filesystem::path& path) const
{
    if (path.empty()) {
        return {};
    }

    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    return ec;
}

bool WalRetentionManager::should_prune_by_time(const WalRetentionConfig& config, const std::filesystem::path& path) const
{
    if (config.retention_hours.count() <= 0) {
        return false;
    }

    std::error_code ec;
    const auto last_write = std::filesystem::last_write_time(path, ec);
    if (ec) {
        return false;
    }

    const auto now = decltype(last_write)::clock::now();
    const auto age = now - last_write;
    const auto age_hours = std::chrono::duration_cast<std::chrono::hours>(age);
    return age_hours >= config.retention_hours;
}

std::error_code WalRetentionManager::prune_segment(const WalRetentionConfig& config,
                                                   const WalSegmentView& segment,
                                                   bool& archived) const
{
    archived = false;
    if (config.archive_path.empty()) {
        std::error_code ec;
        std::filesystem::remove(segment.path, ec);
        return ec;
    }

    if (auto ec = ensure_archive_directory(config.archive_path); ec) {
        return ec;
    }

    auto destination = config.archive_path / segment.path.filename();
    std::error_code rename_ec;
    std::filesystem::remove(destination, rename_ec);

    rename_ec.clear();
    std::filesystem::rename(segment.path, destination, rename_ec);
    if (!rename_ec) {
        archived = true;
    }
    return rename_ec;
}

std::error_code WalRetentionManager::apply(const WalRetentionConfig& config,
                                           std::uint64_t current_segment_id,
                                           WalRetentionStats* stats) const
{
    if (!has_retention_policy(config)) {
        return {};
    }

    if (stats) {
        *stats = WalRetentionStats{};
    }

    SegmentList segments;
    if (auto ec = gather_segments(segments); ec) {
        if (ec == std::errc::no_such_file_or_directory) {
            return {};
        }
        return ec;
    }

    if (segments.empty()) {
        return {};
    }

    if (stats) {
        stats->scanned_segments = static_cast<std::uint64_t>(segments.size());
    }

    std::vector<WalSegmentView> candidates;
    candidates.reserve(segments.size());

    for (const auto& segment : segments) {
        if (segment.header.segment_id >= current_segment_id) {
            continue;
        }
        candidates.push_back(segment);
    }

    if (candidates.empty()) {
        return {};
    }

    if (stats) {
        stats->candidate_segments = static_cast<std::uint64_t>(candidates.size());
    }

    std::unordered_set<std::uint64_t> pruned_ids;

    const auto prune_with_stats = [&](const WalSegmentView& segment) -> std::error_code {
        if (!pruned_ids.insert(segment.header.segment_id).second) {
            return {};
        }
        bool archived = false;
        if (auto ec = prune_segment(config, segment, archived); ec) {
            return ec;
        }
        if (stats) {
            stats->pruned_segments += 1U;
            if (archived) {
                stats->archived_segments += 1U;
            }
        }
        return {};
    };

    const auto retention_count = config.retention_segments;
    if (retention_count > 0U && candidates.size() > retention_count) {
        std::size_t keep_start = candidates.size() - retention_count;
        std::vector<WalSegmentView> prefix(candidates.begin(), candidates.begin() + keep_start);
        for (const auto& segment : prefix) {
            if (auto ec = prune_with_stats(segment); ec) {
                return ec;
            }
        }
    }

    for (const auto& segment : candidates) {
        if (should_prune_by_time(config, segment.path)) {
            if (auto ec = prune_with_stats(segment); ec) {
                return ec;
            }
        }
    }

    return {};
}

}  // namespace bored::storage
