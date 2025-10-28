#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/storage/page_operations.hpp"

#include <memory>
#include <span>

namespace bored::storage {

struct TableScanConfig final {
    catalog::RelationId relation_id{};
    std::uint32_t root_page_id = 0U;
};

struct TableTuple final {
    TupleHeader header{};
    std::span<const std::byte> payload{};
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
};

}  // namespace bored::storage
