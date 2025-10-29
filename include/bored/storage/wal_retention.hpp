#pragma once

#include "bored/storage/wal_durability_horizon.hpp"
#include "bored/storage/wal_reader.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <system_error>
#include <vector>

namespace bored::storage {

struct WalRetentionConfig final {
    std::size_t retention_segments = 0U;
    std::chrono::hours retention_hours{0};
    std::filesystem::path archive_path{};
};

struct WalRetentionStats final {
    std::uint64_t scanned_segments = 0U;
    std::uint64_t candidate_segments = 0U;
    std::uint64_t pruned_segments = 0U;
    std::uint64_t archived_segments = 0U;
};

class WalRetentionManager final {
public:
    class ScopedPin final {
    public:
        ScopedPin() = default;
        explicit ScopedPin(const WalRetentionManager* manager) noexcept;
        ScopedPin(const ScopedPin&) = delete;
        ScopedPin& operator=(const ScopedPin&) = delete;
        ScopedPin(ScopedPin&& other) noexcept;
        ScopedPin& operator=(ScopedPin&& other) noexcept;
        ~ScopedPin();

        [[nodiscard]] bool active() const noexcept;
        void release() noexcept;

    private:
        void move_from(ScopedPin&& other) noexcept;

        const WalRetentionManager* manager_ = nullptr;
        bool active_ = false;
    };

    WalRetentionManager(std::filesystem::path wal_directory,
                        std::string file_prefix,
                        std::string file_extension,
                        std::shared_ptr<WalDurabilityHorizon> durability_horizon = {});

    std::error_code apply(const WalRetentionConfig& config,
                          std::uint64_t current_segment_id,
                          WalRetentionStats* stats = nullptr) const;

    [[nodiscard]] ScopedPin pin() const noexcept;

private:
    using SegmentList = std::vector<WalSegmentView>;

    std::error_code gather_segments(SegmentList& segments) const;
    std::error_code ensure_archive_directory(const std::filesystem::path& path) const;
    bool should_prune_by_time(const WalRetentionConfig& config, const std::filesystem::path& path) const;
    std::error_code prune_segment(const WalRetentionConfig& config,
                                  const WalSegmentView& segment,
                                  bool& archived) const;

    std::filesystem::path wal_directory_{};
    std::string file_prefix_{};
    std::string file_extension_{};
    std::shared_ptr<WalDurabilityHorizon> durability_horizon_{};
    mutable std::atomic<std::uint64_t> active_pins_{0U};
};

}  // namespace bored::storage
