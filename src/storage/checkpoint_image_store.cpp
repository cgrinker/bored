#include "bored/storage/checkpoint_image_store.hpp"

#include "bored/storage/page_format.hpp"

#include <algorithm>
#include <fstream>
#include <string>
#include <string_view>

namespace bored::storage {

namespace {

constexpr std::uint32_t kCheckpointImageMagic = 0x434B5049;  // CKPI
constexpr std::uint16_t kCheckpointImageVersion = 1U;

struct alignas(8) CheckpointImageFileHeader final {
    std::uint32_t magic = kCheckpointImageMagic;
    std::uint16_t version = kCheckpointImageVersion;
    std::uint16_t reserved = 0U;
    std::uint64_t checkpoint_id = 0U;
    std::uint32_t page_count = 0U;
    std::uint32_t reserved0 = 0U;
};

struct alignas(8) CheckpointImagePageHeader final {
    std::uint32_t page_id = 0U;
    std::uint16_t page_type = 0U;
    std::uint16_t reserved = 0U;
    std::uint64_t page_lsn = 0U;
};

std::filesystem::path build_path(const std::filesystem::path& base_directory, std::uint64_t checkpoint_id)
{
    return base_directory / ("checkpoint_" + std::to_string(checkpoint_id) + ".img");
}

}  // namespace

CheckpointImageStore::CheckpointImageStore(std::filesystem::path base_directory) noexcept
    : base_directory_{std::move(base_directory)}
{
}

const std::filesystem::path& CheckpointImageStore::base_directory() const noexcept
{
    return base_directory_;
}

std::error_code CheckpointImageStore::persist(std::uint64_t checkpoint_id,
                                              std::span<const CheckpointPageSnapshot> snapshots) const
{
    if (base_directory_.empty()) {
        return {};
    }

    std::error_code ec;
    std::filesystem::create_directories(base_directory_, ec);
    if (ec) {
        return ec;
    }

    const auto path = build_path(base_directory_, checkpoint_id);
    std::ofstream stream{path, std::ios::binary | std::ios::trunc};
    if (!stream) {
        return std::make_error_code(std::errc::io_error);
    }

    CheckpointImageFileHeader header{};
    header.checkpoint_id = checkpoint_id;
    header.page_count = static_cast<std::uint32_t>(snapshots.size());
    stream.write(reinterpret_cast<const char*>(&header), sizeof(header));
    if (!stream) {
        return std::make_error_code(std::errc::io_error);
    }

    for (const auto& snapshot : snapshots) {
        if (snapshot.image.size() != kPageSize) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        CheckpointImagePageHeader page_header{};
        page_header.page_id = snapshot.entry.page_id;
        page_header.page_type = static_cast<std::uint16_t>(snapshot.page_type);
        page_header.page_lsn = snapshot.entry.page_lsn;
        stream.write(reinterpret_cast<const char*>(&page_header), sizeof(page_header));
        if (!stream) {
            return std::make_error_code(std::errc::io_error);
        }

    stream.write(reinterpret_cast<const char*>(snapshot.image.data()), static_cast<std::streamsize>(snapshot.image.size()));
        if (!stream) {
            return std::make_error_code(std::errc::io_error);
        }
    }

    stream.flush();
    if (!stream) {
        return std::make_error_code(std::errc::io_error);
    }

    return {};
}

std::error_code CheckpointImageStore::load(std::uint64_t checkpoint_id,
                                           std::vector<CheckpointPageSnapshot>& out) const
{
    out.clear();

    if (base_directory_.empty()) {
        return {};
    }

    const auto path = build_path(base_directory_, checkpoint_id);
    std::ifstream stream{path, std::ios::binary};
    if (!stream) {
        return std::make_error_code(std::errc::no_such_file_or_directory);
    }

    CheckpointImageFileHeader header{};
    stream.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!stream) {
        return std::make_error_code(std::errc::io_error);
    }

    if (header.magic != kCheckpointImageMagic || header.version != kCheckpointImageVersion) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    if (header.checkpoint_id != checkpoint_id) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    out.reserve(header.page_count);

    for (std::uint32_t index = 0; index < header.page_count; ++index) {
        CheckpointImagePageHeader page_header{};
        stream.read(reinterpret_cast<char*>(&page_header), sizeof(page_header));
        if (!stream) {
            return std::make_error_code(std::errc::io_error);
        }

        CheckpointPageSnapshot snapshot{};
        snapshot.entry.page_id = page_header.page_id;
        snapshot.entry.page_lsn = page_header.page_lsn;
        snapshot.page_type = static_cast<PageType>(page_header.page_type);
        snapshot.image.resize(kPageSize);
        stream.read(reinterpret_cast<char*>(snapshot.image.data()), static_cast<std::streamsize>(snapshot.image.size()));
        if (!stream) {
            return std::make_error_code(std::errc::io_error);
        }

        out.push_back(std::move(snapshot));
    }

    return {};
}

std::error_code CheckpointImageStore::discard_older_than(std::uint64_t checkpoint_id) const
{
    if (base_directory_.empty()) {
        return {};
    }

    if (!std::filesystem::exists(base_directory_)) {
        return {};
    }

    std::error_code last_error{};

    for (const auto& entry : std::filesystem::directory_iterator(base_directory_)) {
        if (!entry.is_regular_file()) {
            continue;
        }

        const auto name = entry.path().filename().string();
        constexpr std::string_view prefix{"checkpoint_"};
        constexpr std::string_view suffix{".img"};
        if (name.size() <= prefix.size() + suffix.size()) {
            continue;
        }
        if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
            continue;
        }
        auto id_view = name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
        std::uint64_t file_id = 0U;
        try {
            file_id = static_cast<std::uint64_t>(std::stoull(std::string(id_view)));
        } catch (const std::exception&) {
            continue;
        }

        if (file_id >= checkpoint_id) {
            continue;
        }

        std::filesystem::remove(entry.path(), last_error);
        if (last_error) {
            return last_error;
        }
    }

    return {};
}

}  // namespace bored::storage
