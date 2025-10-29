#include "bored/storage/index_retention_executor.hpp"

#include "bored/storage/index_btree_leaf_ops.hpp"
#include "bored/storage/index_btree_page.hpp"
#include "bored/storage/index_comparator_registry.hpp"
#include "bored/storage/page_format.hpp"
#include "bored/storage/page_operations.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <unordered_map>
#include <utility>

namespace bored::storage {
namespace {

[[nodiscard]] bool is_refresh_action(const WalCompactionEntry& entry) noexcept
{
    const auto action = static_cast<WalIndexMaintenanceAction>(entry.index_action);
    return any(action & WalIndexMaintenanceAction::RefreshPointers);
}

}  // namespace

IndexRetentionExecutor::IndexRetentionExecutor() = default;

IndexRetentionExecutor::IndexRetentionExecutor(Config config)
    : config_{std::move(config)}
{
}

std::error_code IndexRetentionExecutor::prune(const IndexRetentionCandidate& candidate)
{
    Config config_copy{};
    {
        std::lock_guard guard{mutex_};
        config_copy = config_;
    }

    const auto run_start = std::chrono::steady_clock::now();
    bool success = false;
    bool skipped = false;
    std::uint64_t scanned_pages = 0U;
    std::uint64_t updated_pointers = 0U;

    auto finalize = [&](std::error_code ec) -> std::error_code {
        if (!ec) {
            success = true;
        }
        record_run(success, skipped, scanned_pages, updated_pointers, run_start);
        return ec;
    };

    if (config_copy.prune_callback) {
        if (auto ec = config_copy.prune_callback(candidate); ec) {
            return finalize(ec);
        }
        scanned_pages = 0U;
        updated_pointers = 0U;
        success = true;
        skipped = false;
        return finalize(std::error_code{});
    }

    if (!config_copy.index_lookup
        || !config_copy.leaf_page_enumerator
        || !config_copy.page_reader
        || !config_copy.page_writer
        || !config_copy.compaction_lookup
        || candidate.level != 0U) {
        skipped = true;
        return finalize(std::error_code{});
    }

    auto descriptor_opt = config_copy.index_lookup(candidate.index_id);
    if (!descriptor_opt.has_value()) {
        skipped = true;
        return finalize(std::error_code{});
    }

    const auto& descriptor = *descriptor_opt;
    const auto* comparator = find_index_comparator(descriptor.comparator);
    if (comparator == nullptr) {
        return finalize(std::make_error_code(std::errc::not_supported));
    }

    auto compaction_entries = config_copy.compaction_lookup(static_cast<std::uint32_t>(candidate.page_id), candidate.prune_lsn);
    std::unordered_map<std::uint16_t, std::uint16_t> pointer_updates;
    pointer_updates.reserve(compaction_entries.size());
    for (const auto& entry : compaction_entries) {
        if (!is_refresh_action(entry)) {
            continue;
        }
        const auto old_offset = static_cast<std::uint16_t>(entry.old_offset);
        const auto new_offset = static_cast<std::uint16_t>(entry.new_offset);
        pointer_updates[old_offset] = new_offset;
    }

    if (pointer_updates.empty()) {
        skipped = true;
        return finalize(std::error_code{});
    }

    auto leaf_pages = config_copy.leaf_page_enumerator(candidate.index_id);
    if (leaf_pages.empty()) {
        skipped = true;
        return finalize(std::error_code{});
    }

    std::array<std::byte, kPageSize> page_buffer{};

    for (const auto page_id : leaf_pages) {
        auto buffer_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
        if (auto ec = config_copy.page_reader(candidate.index_id, page_id, buffer_span); ec) {
            return finalize(ec);
        }

        auto page_span = std::span<const std::byte>(page_buffer.data(), page_buffer.size());
        const auto& header = page_header(page_span);
        if (!is_valid(header)) {
            return finalize(std::make_error_code(std::errc::invalid_argument));
        }
        if (static_cast<PageType>(header.type) != PageType::Index) {
            continue;
        }

        const auto& index_header_ref = *reinterpret_cast<const IndexBtreePageHeader*>(page_buffer.data() + sizeof(PageHeader));
        if (!index_page_is_leaf(index_header_ref)) {
            continue;
        }

        std::error_code parse_error;
        auto entries = read_index_leaf_entries(page_span, parse_error);
        if (parse_error) {
            return finalize(parse_error);
        }

        bool page_modified = false;
        for (auto& entry : entries) {
            if (entry.pointer.heap_page_id != candidate.page_id) {
                continue;
            }
            auto update = pointer_updates.find(entry.pointer.heap_slot_id);
            if (update == pointer_updates.end()) {
                continue;
            }
            if (entry.pointer.heap_slot_id == update->second) {
                continue;
            }
            entry.pointer.heap_slot_id = update->second;
            page_modified = true;
            ++updated_pointers;
        }

        if (page_modified) {
            if (!rebuild_index_leaf_page(buffer_span, std::span<const IndexBtreeLeafEntry>(entries.data(), entries.size()), candidate.prune_lsn)) {
                return finalize(std::make_error_code(std::errc::io_error));
            }
            auto flush_span = std::span<const std::byte>(page_buffer.data(), page_buffer.size());
            if (auto ec = config_copy.page_writer(candidate.index_id, page_id, flush_span, candidate.prune_lsn); ec) {
                return finalize(ec);
            }
        }

        ++scanned_pages;
    }

    return finalize(std::error_code{});
}

void IndexRetentionExecutor::reset()
{
    {
        std::lock_guard guard{mutex_};
        config_ = Config{};
    }
    {
        std::lock_guard telemetry_guard{telemetry_mutex_};
        telemetry_ = {};
    }
}

IndexRetentionExecutor::TelemetrySnapshot IndexRetentionExecutor::telemetry_snapshot() const
{
    std::lock_guard guard{telemetry_mutex_};
    return telemetry_;
}

void IndexRetentionExecutor::record_run(bool success,
                                        bool skipped,
                                        std::uint64_t scanned_pages,
                                        std::uint64_t updated_pointers,
                                        std::chrono::steady_clock::time_point start) noexcept
{
    const auto end = std::chrono::steady_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    const auto duration_ns = duration.count() > 0 ? static_cast<std::uint64_t>(duration.count()) : 0ULL;

    std::lock_guard guard{telemetry_mutex_};
    telemetry_.runs += 1U;
    telemetry_.scanned_pages += scanned_pages;
    telemetry_.updated_pointers += updated_pointers;
    telemetry_.total_duration_ns += duration_ns;
    telemetry_.last_duration_ns = duration_ns;
    if (success) {
        telemetry_.successes += 1U;
    } else {
        telemetry_.failures += 1U;
    }
    if (skipped) {
        telemetry_.skipped_candidates += 1U;
    }
}

}  // namespace bored::storage
