#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/storage/page_operations.hpp"

#include <memory>
#include <span>
#include <vector>

namespace bored::storage {

struct TableScanConfig final {
    catalog::RelationId relation_id{};
    std::uint32_t root_page_id = 0U;
};

struct TableTuple final {
    TupleHeader header{};
    std::span<const std::byte> payload{};
    std::uint32_t page_id = 0U;
    std::uint16_t slot_id = 0U;
};

struct IndexProbeConfig final {
    catalog::RelationId relation_id{};
    catalog::IndexId index_id{};
    std::span<const std::byte> key{};
};

struct IndexProbeResult final {
    TableTuple tuple{};
};

class TableScanCursor {
public:
    virtual ~TableScanCursor() = default;

    virtual bool next(TableTuple& out_tuple) = 0;
    virtual void reset() = 0;
};

class StorageReader {
public:
    virtual ~StorageReader() = default;

    [[nodiscard]] virtual std::unique_ptr<TableScanCursor> create_table_scan(const TableScanConfig& config) = 0;

    [[nodiscard]] virtual std::vector<IndexProbeResult> probe_index(const IndexProbeConfig& config)
    {
        (void)config;
        return {};
    }
};

}  // namespace bored::storage
