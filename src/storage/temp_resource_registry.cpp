#include "bored/storage/temp_resource_registry.hpp"

#include <algorithm>

namespace bored::storage {
namespace {

[[nodiscard]] std::filesystem::path normalise_path(const std::filesystem::path& path)
{
    std::error_code ec;
    auto canonical = std::filesystem::weakly_canonical(path, ec);
    if (!ec) {
        return canonical;
    }
    return path;
}

}  // namespace

void TempResourceRegistry::register_directory(const std::filesystem::path& path)
{
    add_entry(Entry{normalise_path(path), EntryType::Directory});
}

void TempResourceRegistry::register_file(const std::filesystem::path& path)
{
    add_entry(Entry{normalise_path(path), EntryType::File});
}

void TempResourceRegistry::clear()
{
    std::lock_guard guard(mutex_);
    entries_.clear();
}

bool TempResourceRegistry::empty() const noexcept
{
    std::lock_guard guard(mutex_);
    return entries_.empty();
}

std::error_code TempResourceRegistry::purge(TempResourcePurgeReason /*reason*/, TempResourcePurgeStats* stats) const noexcept
{
    std::vector<Entry> snapshot;
    {
        std::lock_guard guard(mutex_);
        snapshot = entries_;
    }

    if (stats != nullptr) {
        *stats = TempResourcePurgeStats{};
    }

    std::error_code first_error{};

    for (const auto& entry : snapshot) {
        std::uint64_t purged = 0U;
        std::error_code purge_error;
        switch (entry.type) {
            case EntryType::Directory:
                purge_error = purge_directory(entry.path, purged);
                break;
            case EntryType::File:
                purge_error = purge_file(entry.path, purged);
                break;
        }

        if (stats != nullptr) {
            stats->checked_entries += 1U;
            stats->purged_entries += purged;
            if (purge_error) {
                stats->errors += 1U;
            }
        }

        if (purge_error && !first_error) {
            first_error = purge_error;
        }
    }

    return first_error;
}

void TempResourceRegistry::add_entry(Entry entry)
{
    std::lock_guard guard(mutex_);
    auto it = std::find_if(entries_.begin(), entries_.end(), [&](const Entry& existing) {
        return existing.path == entry.path;
    });
    if (it == entries_.end()) {
        entries_.push_back(std::move(entry));
    } else {
        it->type = entry.type;
    }
}

std::error_code TempResourceRegistry::purge_directory(const std::filesystem::path& path,
                                                      std::uint64_t& purged) const
{
    std::error_code ec;
    auto exists = std::filesystem::exists(path, ec);
    if (ec) {
        return ec;
    }
    if (!exists) {
        return {};
    }
    if (!std::filesystem::is_directory(path, ec)) {
        if (ec) {
            return ec;
        }
        return std::make_error_code(std::errc::not_a_directory);
    }

    std::error_code iterator_error;
    for (auto it = std::filesystem::directory_iterator(path, iterator_error); !iterator_error && it != std::filesystem::directory_iterator{}; ++it) {
        std::error_code remove_error;
        auto removed = std::filesystem::remove_all(it->path(), remove_error);
        if (remove_error) {
            if (!ec) {
                ec = remove_error;
            }
        } else {
            purged += static_cast<std::uint64_t>(removed);
        }
    }

    if (iterator_error && !ec) {
        ec = iterator_error;
    }

    return ec;
}

std::error_code TempResourceRegistry::purge_file(const std::filesystem::path& path, std::uint64_t& purged) const
{
    std::error_code ec;
    auto exists = std::filesystem::exists(path, ec);
    if (ec) {
        return ec;
    }
    if (!exists) {
        return {};
    }

    if (std::filesystem::is_directory(path, ec)) {
        if (ec) {
            return ec;
        }
        return std::make_error_code(std::errc::is_a_directory);
    }

    std::error_code remove_error;
    if (std::filesystem::remove(path, remove_error)) {
        purged += 1U;
    }
    if (remove_error) {
        return remove_error;
    }

    return {};
}

}  // namespace bored::storage
