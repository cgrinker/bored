#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

namespace bored::storage {

class FreeSpaceMap final {
public:
    static constexpr std::uint16_t kBucketSize = 128U;
    static constexpr std::size_t kBucketCount = (8192U / kBucketSize) + 1U;  // final bucket for overflow pages.

    struct SnapshotEntry final {
        std::uint32_t page_id = 0U;
        std::uint16_t free_bytes = 0U;
        std::uint16_t fragment_count = 0U;
    };

    FreeSpaceMap();

    void record_page(std::uint32_t page_id, std::uint16_t free_bytes, std::uint16_t fragment_count);
    void remove_page(std::uint32_t page_id);

    [[nodiscard]] std::optional<std::uint32_t> find_page_with_space(std::uint16_t required_bytes) const;
    [[nodiscard]] std::uint16_t current_free_bytes(std::uint32_t page_id) const;
    [[nodiscard]] std::uint16_t current_fragment_count(std::uint32_t page_id) const;

    void clear();

    void rebuild_from_snapshot(std::span<const SnapshotEntry> entries);
    void for_each(const std::function<void(const SnapshotEntry&)>& visitor) const;

private:
    struct PageInfo {
        std::uint16_t free_bytes = 0U;
        std::uint16_t fragment_count = 0U;
        std::size_t bucket_index = 0U;
        std::size_t bucket_offset = 0U;
    };

    using Bucket = std::vector<std::uint32_t>;

    std::array<Bucket, kBucketCount> buckets_{};
    std::unordered_map<std::uint32_t, PageInfo> pages_;

    static std::size_t bucket_for(std::uint16_t free_bytes);
    void detach_from_bucket(std::uint32_t page_id, PageInfo& info);
    void attach_to_bucket(std::uint32_t page_id, PageInfo& info, std::size_t bucket_index);
};

}  // namespace bored::storage
