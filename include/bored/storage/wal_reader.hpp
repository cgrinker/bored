#pragma once

#include "bored/storage/wal_format.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <span>
#include <string>
#include <system_error>
#include <vector>

namespace bored::storage {

struct WalRecordView final {
    WalRecordHeader header{};
    std::vector<std::byte> payload{};
};

struct WalSegmentView final {
    WalSegmentHeader header{};
    std::filesystem::path path{};
};

class WalReader final {
public:
    WalReader(std::filesystem::path directory,
              std::string file_prefix = "wal",
              std::string file_extension = ".seg");

    [[nodiscard]] std::error_code enumerate_segments(std::vector<WalSegmentView>& segments) const;

    [[nodiscard]] std::error_code read_records(const WalSegmentView& segment,
                                               std::vector<WalRecordView>& records) const;

    [[nodiscard]] std::error_code for_each_record(const std::function<bool(const WalSegmentView&, const WalRecordHeader&, std::span<const std::byte>)>& callback) const;

private:
    [[nodiscard]] std::error_code read_segment_header(std::ifstream& stream, WalSegmentHeader& header) const;
    [[nodiscard]] std::error_code read_record(std::ifstream& stream,
                                              WalRecordHeader& header,
                                              std::vector<std::byte>& payload) const;

    std::filesystem::path directory_{};
    std::string file_prefix_{};
    std::string file_extension_{};
};

}  // namespace bored::storage
