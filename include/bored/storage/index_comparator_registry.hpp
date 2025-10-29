#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace bored::storage {

struct IndexComparatorEntry final {
    using CompareFn = std::strong_ordering (*)(std::span<const std::byte>, std::span<const std::byte>) noexcept;

    std::string name{};
    CompareFn compare = nullptr;
    std::size_t fixed_length = 0U;  // zero indicates variable length keys
};

class IndexComparatorRegistry final {
public:
    IndexComparatorRegistry();

    IndexComparatorRegistry(const IndexComparatorRegistry&) = delete;
    IndexComparatorRegistry& operator=(const IndexComparatorRegistry&) = delete;
    IndexComparatorRegistry(IndexComparatorRegistry&&) = delete;
    IndexComparatorRegistry& operator=(IndexComparatorRegistry&&) = delete;

    static IndexComparatorRegistry& instance() noexcept;

    bool register_comparator(IndexComparatorEntry entry);

    [[nodiscard]] const IndexComparatorEntry* find(std::string_view name) const noexcept;
    [[nodiscard]] std::vector<std::string> names() const;

private:
    bool register_builtins();

    mutable std::mutex mutex_;
    std::vector<IndexComparatorEntry> entries_{};
    bool builtins_registered_ = false;
};

bool register_index_comparator(IndexComparatorEntry entry);
[[nodiscard]] const IndexComparatorEntry* find_index_comparator(std::string_view name) noexcept;
[[nodiscard]] std::vector<std::string> index_comparator_names();

}  // namespace bored::storage
