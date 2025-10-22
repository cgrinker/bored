#pragma once

#include "bored/storage/free_space_map.hpp"

#include <filesystem>
#include <system_error>

namespace bored::storage {

class FreeSpaceMapPersistence final {
public:
    static constexpr std::uint32_t kMagic = 0x46534D31;  // 'FSM1'
    static constexpr std::uint32_t kVersion = 1U;

    [[nodiscard]] static std::error_code write_snapshot(const FreeSpaceMap& fsm, const std::filesystem::path& path);
    [[nodiscard]] static std::error_code load_snapshot(const std::filesystem::path& path, FreeSpaceMap& fsm);
};

}  // namespace bored::storage
