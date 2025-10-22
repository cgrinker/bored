#include "bored/storage/wal_reader.hpp"

#include "bored/storage/checksum.hpp"

#include <algorithm>
#include <vector>

namespace bored::storage {

namespace {

bool has_matching_name(const std::filesystem::directory_entry& entry,
                       const std::string& prefix,
                       const std::string& extension)
{
    if (!entry.is_regular_file()) {
        return false;
    }

    const auto filename = entry.path().filename().string();
    if (!prefix.empty()) {
        if (filename.rfind(prefix + '_', 0) != 0) {
            return false;
        }
    }

    if (!extension.empty()) {
        if (entry.path().extension() != extension) {
            return false;
        }
    }

    return true;
}

std::error_code seek_forward(std::ifstream& stream, std::streamoff amount)
{
    stream.seekg(amount, std::ios::cur);
    if (!stream) {
        return std::make_error_code(std::errc::io_error);
    }
    return {};
}

}  // namespace

WalReader::WalReader(std::filesystem::path directory,
                     std::string file_prefix,
                     std::string file_extension)
    : directory_{std::move(directory)}
    , file_prefix_{std::move(file_prefix)}
    , file_extension_{std::move(file_extension)}
{
}

std::error_code WalReader::enumerate_segments(std::vector<WalSegmentView>& segments) const
{
    segments.clear();

    if (!std::filesystem::exists(directory_)) {
        return std::make_error_code(std::errc::no_such_file_or_directory);
    }

    for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
        if (!has_matching_name(entry, file_prefix_, file_extension_)) {
            continue;
        }

        WalSegmentHeader header{};
        std::ifstream stream(entry.path(), std::ios::binary);
        if (!stream) {
            return std::make_error_code(std::errc::io_error);
        }

        if (auto ec = read_segment_header(stream, header); ec) {
            return ec;
        }

        segments.push_back(WalSegmentView{header, entry.path()});
    }

    std::sort(segments.begin(), segments.end(), [](const WalSegmentView& lhs, const WalSegmentView& rhs) {
        return lhs.header.segment_id < rhs.header.segment_id;
    });

    return {};
}

std::error_code WalReader::read_records(const WalSegmentView& segment, std::vector<WalRecordView>& records) const
{
    records.clear();

    std::ifstream stream(segment.path, std::ios::binary);
    if (!stream) {
        return std::make_error_code(std::errc::io_error);
    }

    WalSegmentHeader header{};
    if (auto ec = read_segment_header(stream, header); ec) {
        return ec;
    }

    const auto header_padding = static_cast<std::streamoff>(kWalBlockSize - sizeof(WalSegmentHeader));
    if (header_padding > 0) {
        if (auto ec = seek_forward(stream, header_padding); ec) {
            return ec;
        }
    }

    const auto end_offset = static_cast<std::streamoff>(segment.header.end_lsn - segment.header.start_lsn);
    std::streamoff processed = 0;

    while (stream) {
        WalRecordHeader record_header{};
        std::vector<std::byte> payload{};

        auto current_pos = stream.tellg();
        if (!stream) {
            break;
        }

        if (auto ec = read_record(stream, record_header, payload); ec) {
            if (record_header.total_length == 0U) {
                break;
            }
            return ec;
        }

        if (record_header.total_length == 0U) {
            break;
        }

        if (!is_valid_record_header(record_header)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        if (!verify_wal_checksum(record_header, payload)) {
            return std::make_error_code(std::errc::io_error);
        }

        processed += align_up_to_block(static_cast<std::size_t>(record_header.total_length));
        if (end_offset != 0 && processed > end_offset) {
            break;
        }

        records.push_back(WalRecordView{record_header, std::move(payload)});
    }

    return {};
}

std::error_code WalReader::for_each_record(const std::function<bool(const WalSegmentView&, const WalRecordHeader&, std::span<const std::byte>)>& callback) const
{
    std::vector<WalSegmentView> segments;
    if (auto ec = enumerate_segments(segments); ec) {
        return ec;
    }

    for (const auto& segment : segments) {
        std::ifstream stream(segment.path, std::ios::binary);
        if (!stream) {
            return std::make_error_code(std::errc::io_error);
        }

        WalSegmentHeader header{};
        if (auto ec = read_segment_header(stream, header); ec) {
            return ec;
        }

        const auto header_padding = static_cast<std::streamoff>(kWalBlockSize - sizeof(WalSegmentHeader));
        if (header_padding > 0) {
            if (auto ec = seek_forward(stream, header_padding); ec) {
                return ec;
            }
        }

        while (stream) {
            WalRecordHeader record_header{};
            std::vector<std::byte> payload{};

            auto ec = read_record(stream, record_header, payload);
            if (ec) {
                if (record_header.total_length == 0U) {
                    break;
                }
                return ec;
            }

            if (record_header.total_length == 0U) {
                break;
            }

            if (!is_valid_record_header(record_header)) {
                return std::make_error_code(std::errc::invalid_argument);
            }

            if (!verify_wal_checksum(record_header, payload)) {
                return std::make_error_code(std::errc::io_error);
            }

            if (!callback(segment, record_header, std::span<const std::byte>(payload))) {
                return {};
            }
        }
    }

    return {};
}

std::error_code WalReader::read_segment_header(std::ifstream& stream, WalSegmentHeader& header) const
{
    stream.read(reinterpret_cast<char*>(&header), sizeof(WalSegmentHeader));
    if (stream.gcount() != static_cast<std::streamsize>(sizeof(WalSegmentHeader))) {
        return std::make_error_code(std::errc::io_error);
    }

    if (!is_valid_segment_header(header)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    return {};
}

std::error_code WalReader::read_record(std::ifstream& stream,
                                       WalRecordHeader& header,
                                       std::vector<std::byte>& payload) const
{
    header = {};
    payload.clear();

    stream.read(reinterpret_cast<char*>(&header), sizeof(WalRecordHeader));
    if (stream.gcount() == 0) {
        header.total_length = 0U;
        return {};
    }

    if (stream.gcount() != static_cast<std::streamsize>(sizeof(WalRecordHeader))) {
        return std::make_error_code(std::errc::io_error);
    }

    if (header.total_length == 0U) {
        return {};
    }

    if (header.total_length < sizeof(WalRecordHeader)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto payload_size = static_cast<std::size_t>(header.total_length - sizeof(WalRecordHeader));
    payload.resize(payload_size);
    stream.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    if (static_cast<std::size_t>(stream.gcount()) != payload_size) {
        return std::make_error_code(std::errc::io_error);
    }

    const auto aligned = align_up_to_block(static_cast<std::size_t>(header.total_length));
    const auto padding = static_cast<std::streamoff>(aligned - header.total_length);
    if (padding > 0) {
        if (auto ec = seek_forward(stream, padding); ec) {
            return ec;
        }
    }

    return {};
}

}  // namespace bored::storage
