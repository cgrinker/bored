#include "bored/storage/free_space_map.hpp"

#include <algorithm>

namespace bored::storage {

FreeSpaceMap::FreeSpaceMap() = default;

std::size_t FreeSpaceMap::bucket_for(std::uint16_t free_bytes)
{
    const auto bucket = static_cast<std::size_t>(free_bytes / kBucketSize);
    return std::min(bucket, kBucketCount - 1U);
}

void FreeSpaceMap::detach_from_bucket(std::uint32_t page_id, PageInfo& info)
{
    auto& bucket = buckets_[info.bucket_index];
    if (bucket.empty()) {
        return;
    }

    const auto index = info.bucket_offset;
    const auto last = bucket.back();
    bucket[index] = last;
    bucket.pop_back();

    if (last != page_id) {
        auto& moved = pages_.at(last);
        moved.bucket_offset = index;
    }
}

void FreeSpaceMap::attach_to_bucket(std::uint32_t page_id, PageInfo& info, std::size_t bucket_index)
{
    auto& bucket = buckets_[bucket_index];
    info.bucket_index = bucket_index;
    info.bucket_offset = bucket.size();
    bucket.push_back(page_id);
}

void FreeSpaceMap::record_page(std::uint32_t page_id, std::uint16_t free_bytes, std::uint16_t fragment_count)
{
    auto iter = pages_.find(page_id);
    if (iter == pages_.end()) {
        PageInfo info{};
        info.free_bytes = free_bytes;
        info.fragment_count = fragment_count;
        attach_to_bucket(page_id, info, bucket_for(free_bytes));
        pages_.emplace(page_id, info);
        return;
    }

    auto& info = iter->second;
    detach_from_bucket(page_id, info);
    info.free_bytes = free_bytes;
    info.fragment_count = fragment_count;
    attach_to_bucket(page_id, info, bucket_for(free_bytes));
}

void FreeSpaceMap::remove_page(std::uint32_t page_id)
{
    auto iter = pages_.find(page_id);
    if (iter == pages_.end()) {
        return;
    }

    detach_from_bucket(page_id, iter->second);
    pages_.erase(iter);
}

std::optional<std::uint32_t> FreeSpaceMap::find_page_with_space(std::uint16_t required_bytes) const
{
    const auto start_bucket = bucket_for(required_bytes);
    for (std::size_t bucket = start_bucket; bucket < kBucketCount; ++bucket) {
        const auto& entries = buckets_[bucket];
        if (!entries.empty()) {
            std::optional<std::uint32_t> fragmented_candidate;
            for (auto iter = entries.rbegin(); iter != entries.rend(); ++iter) {
                const auto page_id = *iter;
                const auto page_info = pages_.find(page_id);
                if (page_info == pages_.end()) {
                    continue;
                }

                if (page_info->second.fragment_count == 0U) {
                    return page_id;
                }

                if (!fragmented_candidate) {
                    fragmented_candidate = page_id;
                }
            }

            if (fragmented_candidate) {
                return fragmented_candidate;
            }
        }
    }
    return std::nullopt;
}

std::uint16_t FreeSpaceMap::current_free_bytes(std::uint32_t page_id) const
{
    auto iter = pages_.find(page_id);
    if (iter == pages_.end()) {
        return 0U;
    }
    return iter->second.free_bytes;
}

std::uint16_t FreeSpaceMap::current_fragment_count(std::uint32_t page_id) const
{
    auto iter = pages_.find(page_id);
    if (iter == pages_.end()) {
        return 0U;
    }
    return iter->second.fragment_count;
}

void FreeSpaceMap::clear()
{
    for (auto& bucket : buckets_) {
        bucket.clear();
    }
    pages_.clear();
}

void FreeSpaceMap::rebuild_from_snapshot(std::span<const SnapshotEntry> entries)
{
    clear();
    for (const auto& entry : entries) {
        record_page(entry.page_id, entry.free_bytes, entry.fragment_count);
    }
}

void FreeSpaceMap::for_each(const std::function<void(const SnapshotEntry&)>& visitor) const
{
    if (!visitor) {
        return;
    }
    for (const auto& [page_id, info] : pages_) {
        visitor(SnapshotEntry{page_id, info.free_bytes, info.fragment_count});
    }
}

}  // namespace bored::storage
