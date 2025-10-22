#include "bored/storage/free_space_map_persistence.hpp"

#include <fstream>
#include <vector>

namespace bored::storage {

namespace {

struct SnapshotHeader final {
    std::uint32_t magic = FreeSpaceMapPersistence::kMagic;
    std::uint32_t version = FreeSpaceMapPersistence::kVersion;
    std::uint32_t entry_count = 0U;
};

}  // namespace

std::error_code FreeSpaceMapPersistence::write_snapshot(const FreeSpaceMap& fsm, const std::filesystem::path& path)
{
    std::vector<FreeSpaceMap::SnapshotEntry> entries;
    fsm.for_each([&](const FreeSpaceMap::SnapshotEntry& entry) {
        entries.push_back(entry);
    });

    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    if (!stream) {
        return std::make_error_code(std::errc::io_error);
    }

    SnapshotHeader header{};
    header.entry_count = static_cast<std::uint32_t>(entries.size());
    stream.write(reinterpret_cast<const char*>(&header), sizeof(header));
    if (!stream) {
        return std::make_error_code(std::errc::io_error);
    }

    for (const auto& entry : entries) {
        stream.write(reinterpret_cast<const char*>(&entry), sizeof(entry));
        if (!stream) {
            return std::make_error_code(std::errc::io_error);
        }
    }

    return {};
}

std::error_code FreeSpaceMapPersistence::load_snapshot(const std::filesystem::path& path, FreeSpaceMap& fsm)
{
    std::ifstream stream(path, std::ios::binary);
    if (!stream) {
        return std::make_error_code(std::errc::no_such_file_or_directory);
    }

    SnapshotHeader header{};
    stream.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (stream.gcount() != static_cast<std::streamsize>(sizeof(header))) {
        return std::make_error_code(std::errc::io_error);
    }

    if (header.magic != kMagic || header.version != kVersion) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    std::vector<FreeSpaceMap::SnapshotEntry> entries(header.entry_count);
    for (std::uint32_t index = 0; index < header.entry_count; ++index) {
        stream.read(reinterpret_cast<char*>(&entries[index]), sizeof(FreeSpaceMap::SnapshotEntry));
        if (stream.gcount() != static_cast<std::streamsize>(sizeof(FreeSpaceMap::SnapshotEntry))) {
            return std::make_error_code(std::errc::io_error);
        }
    }

    fsm.rebuild_from_snapshot(entries);
    return {};
}

}  // namespace bored::storage
