#include "bored/storage/index_comparator_registry.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <utility>

namespace bored::storage {
namespace {

std::strong_ordering compare_bytes(std::span<const std::byte> lhs, std::span<const std::byte> rhs) noexcept
{
    const auto min_length = std::min(lhs.size(), rhs.size());
    if (min_length > 0U) {
        const auto result = std::memcmp(lhs.data(), rhs.data(), min_length);
        if (result < 0) {
            return std::strong_ordering::less;
        }
        if (result > 0) {
            return std::strong_ordering::greater;
        }
    }
    if (lhs.size() < rhs.size()) {
        return std::strong_ordering::less;
    }
    if (lhs.size() > rhs.size()) {
        return std::strong_ordering::greater;
    }
    return std::strong_ordering::equal;
}

std::strong_ordering compare_utf8(std::span<const std::byte> lhs, std::span<const std::byte> rhs) noexcept
{
    const auto lhs_view = std::string_view(reinterpret_cast<const char*>(lhs.data()), lhs.size());
    const auto rhs_view = std::string_view(reinterpret_cast<const char*>(rhs.data()), rhs.size());
    const auto result = lhs_view.compare(rhs_view);
    if (result < 0) {
        return std::strong_ordering::less;
    }
    if (result > 0) {
        return std::strong_ordering::greater;
    }
    return std::strong_ordering::equal;
}

template <typename Numeric>
std::strong_ordering compare_numeric(std::span<const std::byte> lhs, std::span<const std::byte> rhs) noexcept
{
    if (lhs.size() != sizeof(Numeric) || rhs.size() != sizeof(Numeric)) {
        return compare_bytes(lhs, rhs);
    }

    Numeric lhs_value{};
    Numeric rhs_value{};
    std::memcpy(&lhs_value, lhs.data(), sizeof(Numeric));
    std::memcpy(&rhs_value, rhs.data(), sizeof(Numeric));

    if (lhs_value < rhs_value) {
        return std::strong_ordering::less;
    }
    if (lhs_value > rhs_value) {
        return std::strong_ordering::greater;
    }
    return std::strong_ordering::equal;
}

IndexComparatorEntry make_entry(std::string_view name,
                                IndexComparatorEntry::CompareFn fn,
                                std::size_t fixed_length = 0U)
{
    IndexComparatorEntry entry{};
    entry.name = name;
    entry.compare = fn;
    entry.fixed_length = fixed_length;
    return entry;
}

}  // namespace

IndexComparatorRegistry::IndexComparatorRegistry()
{
    register_builtins();
}

IndexComparatorRegistry& IndexComparatorRegistry::instance() noexcept
{
    static IndexComparatorRegistry registry;
    return registry;
}

bool IndexComparatorRegistry::register_comparator(IndexComparatorEntry entry)
{
    if (entry.name.empty() || entry.compare == nullptr) {
        return false;
    }

    std::scoped_lock lock(mutex_);
    auto it = std::find_if(entries_.begin(), entries_.end(), [&](const IndexComparatorEntry& existing) {
        return existing.name == entry.name;
    });
    if (it != entries_.end()) {
        return false;
    }
    entries_.push_back(std::move(entry));
    return true;
}

const IndexComparatorEntry* IndexComparatorRegistry::find(std::string_view name) const noexcept
{
    std::scoped_lock lock(mutex_);
    auto it = std::find_if(entries_.begin(), entries_.end(), [&](const IndexComparatorEntry& entry) {
        return entry.name == name;
    });
    if (it == entries_.end()) {
        return nullptr;
    }
    return &(*it);
}

std::vector<std::string> IndexComparatorRegistry::names() const
{
    std::scoped_lock lock(mutex_);
    std::vector<std::string> result;
    result.reserve(entries_.size());
    for (const auto& entry : entries_) {
        result.push_back(entry.name);
    }
    std::sort(result.begin(), result.end());
    return result;
}

bool IndexComparatorRegistry::register_builtins()
{
    std::scoped_lock lock(mutex_);
    if (builtins_registered_) {
        return true;
    }

    const std::array<IndexComparatorEntry, 5> builtins{
        make_entry("bytelex_ascending", compare_bytes),
        make_entry("utf8_ascending", compare_utf8),
        make_entry("int64_ascending", compare_numeric<std::int64_t>, sizeof(std::int64_t)),
        make_entry("uint32_ascending", compare_numeric<std::uint32_t>, sizeof(std::uint32_t)),
        make_entry("uint16_ascending", compare_numeric<std::uint16_t>, sizeof(std::uint16_t))
    };

    for (const auto& entry : builtins) {
        auto duplicate = std::find_if(entries_.begin(), entries_.end(), [&](const IndexComparatorEntry& existing) {
            return existing.name == entry.name;
        });
        if (duplicate == entries_.end()) {
            entries_.push_back(entry);
        }
    }

    builtins_registered_ = true;
    return true;
}

bool register_index_comparator(IndexComparatorEntry entry)
{
    return IndexComparatorRegistry::instance().register_comparator(std::move(entry));
}

const IndexComparatorEntry* find_index_comparator(std::string_view name) noexcept
{
    return IndexComparatorRegistry::instance().find(name);
}

std::vector<std::string> index_comparator_names()
{
    return IndexComparatorRegistry::instance().names();
}

}  // namespace bored::storage
