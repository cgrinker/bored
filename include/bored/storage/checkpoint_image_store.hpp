#pragma once

#include "bored/storage/checkpoint_types.hpp"

#include <filesystem>
#include <span>
#include <system_error>
#include <vector>

namespace bored::storage {

class CheckpointImageStore final {
public:
    explicit CheckpointImageStore(std::filesystem::path base_directory) noexcept;

    [[nodiscard]] const std::filesystem::path& base_directory() const noexcept;

    [[nodiscard]] std::error_code persist(std::uint64_t checkpoint_id,
                                          std::span<const CheckpointPageSnapshot> snapshots) const;

    [[nodiscard]] std::error_code load(std::uint64_t checkpoint_id,
                                       std::vector<CheckpointPageSnapshot>& out) const;

    [[nodiscard]] std::error_code discard_older_than(std::uint64_t checkpoint_id) const;

private:
    std::filesystem::path base_directory_{};
};

}  // namespace bored::storage
