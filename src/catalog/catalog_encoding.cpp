#include "bored/catalog/catalog_encoding.hpp"

#include <algorithm>
#include <cstring>

namespace bored::catalog {

namespace {

constexpr std::size_t align8(std::size_t value) noexcept
{
    return (value + 7U) & ~std::size_t{7U};
}

struct CatalogDatabasePrefix final {
    CatalogTupleDescriptor tuple{};
    std::uint64_t database_id = 0U;
    std::uint64_t default_schema_id = 0U;
    std::uint16_t name_length = 0U;
    std::uint16_t reserved = 0U;
    std::uint32_t options = 0U;
};

struct CatalogSchemaPrefix final {
    CatalogTupleDescriptor tuple{};
    std::uint64_t schema_id = 0U;
    std::uint64_t database_id = 0U;
    std::uint16_t name_length = 0U;
    std::uint16_t reserved = 0U;
    std::uint32_t padding = 0U;
};

struct CatalogTablePrefix final {
    CatalogTupleDescriptor tuple{};
    std::uint64_t relation_id = 0U;
    std::uint64_t schema_id = 0U;
    std::uint16_t table_type = 0U;
    std::uint16_t reserved = 0U;
    std::uint32_t root_page_id = 0U;
    std::uint16_t name_length = 0U;
    std::uint16_t padding = 0U;
};

struct CatalogColumnPrefix final {
    CatalogTupleDescriptor tuple{};
    std::uint64_t column_id = 0U;
    std::uint64_t relation_id = 0U;
    std::uint32_t column_type = 0U;
    std::uint16_t ordinal_position = 0U;
    std::uint16_t name_length = 0U;
    std::uint32_t padding = 0U;
};

struct CatalogIndexPrefix final {
    CatalogTupleDescriptor tuple{};
    std::uint64_t index_id = 0U;
    std::uint64_t relation_id = 0U;
    std::uint16_t index_type = 0U;
    std::uint16_t max_fanout = 0U;
    std::uint32_t root_page_id = 0U;
    std::uint16_t name_length = 0U;
    std::uint16_t comparator_length = 0U;
    std::uint32_t padding = 0U;
};

static_assert(sizeof(CatalogDatabasePrefix) == 48U, "CatalogDatabasePrefix expected to be 48 bytes");
static_assert(sizeof(CatalogSchemaPrefix) == 48U, "CatalogSchemaPrefix expected to be 48 bytes");
static_assert(sizeof(CatalogTablePrefix) == 56U, "CatalogTablePrefix expected to be 56 bytes");
static_assert(sizeof(CatalogColumnPrefix) == 56U, "CatalogColumnPrefix expected to be 56 bytes");
static_assert(sizeof(CatalogIndexPrefix) == 56U, "CatalogIndexPrefix expected to be 56 bytes");

std::vector<std::byte> serialize_prefixed_tuple(std::size_t prefix_size, std::string_view name)
{
    const auto size = align8(prefix_size + name.size());
    std::vector<std::byte> buffer(size, std::byte{0});
    if (!name.empty()) {
        std::memcpy(buffer.data() + prefix_size, name.data(), name.size());
    }
    return buffer;
}

}  // namespace

std::size_t catalog_database_tuple_size(std::string_view name) noexcept
{
    return align8(sizeof(CatalogDatabasePrefix) + name.size());
}

std::size_t catalog_schema_tuple_size(std::string_view name) noexcept
{
    return align8(sizeof(CatalogSchemaPrefix) + name.size());
}

std::size_t catalog_table_tuple_size(std::string_view name) noexcept
{
    return align8(sizeof(CatalogTablePrefix) + name.size());
}

std::size_t catalog_column_tuple_size(std::string_view name) noexcept
{
    return align8(sizeof(CatalogColumnPrefix) + name.size());
}

std::size_t catalog_index_tuple_size(std::string_view name, std::string_view comparator) noexcept
{
    return align8(sizeof(CatalogIndexPrefix) + name.size() + comparator.size());
}

std::vector<std::byte> serialize_catalog_database(const CatalogDatabaseDescriptor& descriptor)
{
    auto buffer = serialize_prefixed_tuple(sizeof(CatalogDatabasePrefix), descriptor.name);
    auto* prefix = reinterpret_cast<CatalogDatabasePrefix*>(buffer.data());
    prefix->tuple = descriptor.tuple;
    prefix->database_id = descriptor.database_id.value;
    prefix->default_schema_id = descriptor.default_schema_id.value;
    prefix->name_length = static_cast<std::uint16_t>(descriptor.name.size());
    return buffer;
}

std::vector<std::byte> serialize_catalog_schema(const CatalogSchemaDescriptor& descriptor)
{
    auto buffer = serialize_prefixed_tuple(sizeof(CatalogSchemaPrefix), descriptor.name);
    auto* prefix = reinterpret_cast<CatalogSchemaPrefix*>(buffer.data());
    prefix->tuple = descriptor.tuple;
    prefix->schema_id = descriptor.schema_id.value;
    prefix->database_id = descriptor.database_id.value;
    prefix->name_length = static_cast<std::uint16_t>(descriptor.name.size());
    return buffer;
}

std::vector<std::byte> serialize_catalog_table(const CatalogTableDescriptor& descriptor)
{
    auto buffer = serialize_prefixed_tuple(sizeof(CatalogTablePrefix), descriptor.name);
    auto* prefix = reinterpret_cast<CatalogTablePrefix*>(buffer.data());
    prefix->tuple = descriptor.tuple;
    prefix->relation_id = descriptor.relation_id.value;
    prefix->schema_id = descriptor.schema_id.value;
    prefix->table_type = static_cast<std::uint16_t>(descriptor.table_type);
    prefix->root_page_id = descriptor.root_page_id;
    prefix->name_length = static_cast<std::uint16_t>(descriptor.name.size());
    return buffer;
}

std::vector<std::byte> serialize_catalog_column(const CatalogColumnDescriptor& descriptor)
{
    auto buffer = serialize_prefixed_tuple(sizeof(CatalogColumnPrefix), descriptor.name);
    auto* prefix = reinterpret_cast<CatalogColumnPrefix*>(buffer.data());
    prefix->tuple = descriptor.tuple;
    prefix->column_id = descriptor.column_id.value;
    prefix->relation_id = descriptor.relation_id.value;
    prefix->column_type = static_cast<std::uint32_t>(descriptor.column_type);
    prefix->ordinal_position = descriptor.ordinal_position;
    prefix->name_length = static_cast<std::uint16_t>(descriptor.name.size());
    return buffer;
}

std::vector<std::byte> serialize_catalog_index(const CatalogIndexDescriptor& descriptor)
{
    const auto name_length = static_cast<std::uint16_t>(descriptor.name.size());
    const auto comparator_length = static_cast<std::uint16_t>(descriptor.comparator.size());
    const auto prefix_size = sizeof(CatalogIndexPrefix);
    auto buffer = std::vector<std::byte>(align8(prefix_size + descriptor.name.size() + descriptor.comparator.size()), std::byte{0});
    auto* prefix = reinterpret_cast<CatalogIndexPrefix*>(buffer.data());
    prefix->tuple = descriptor.tuple;
    prefix->index_id = descriptor.index_id.value;
    prefix->relation_id = descriptor.relation_id.value;
    prefix->index_type = static_cast<std::uint16_t>(descriptor.index_type);
    prefix->max_fanout = descriptor.max_fanout;
    prefix->root_page_id = descriptor.root_page_id;
    prefix->name_length = name_length;
    prefix->comparator_length = comparator_length;
    if (name_length > 0U) {
        std::memcpy(buffer.data() + prefix_size, descriptor.name.data(), descriptor.name.size());
    }
    if (comparator_length > 0U) {
        std::memcpy(buffer.data() + prefix_size + descriptor.name.size(), descriptor.comparator.data(), descriptor.comparator.size());
    }
    return buffer;
}

namespace {

template <typename Prefix>
std::string_view read_name(const Prefix& prefix, std::span<const std::byte> tuple)
{
    if (prefix.name_length == 0U) {
        return {};
    }
    const auto prefix_size = sizeof(Prefix);
    if (tuple.size() < prefix_size || tuple.size() < prefix_size + prefix.name_length) {
        return {};
    }
    const char* data = reinterpret_cast<const char*>(tuple.data() + prefix_size);
    return {data, prefix.name_length};
}

}  // namespace

std::optional<CatalogDatabaseView> decode_catalog_database(std::span<const std::byte> tuple)
{
    if (tuple.size() < sizeof(CatalogDatabasePrefix)) {
        return std::nullopt;
    }
    const auto* prefix = reinterpret_cast<const CatalogDatabasePrefix*>(tuple.data());
    CatalogDatabaseView view{};
    view.tuple = prefix->tuple;
    view.database_id = DatabaseId{prefix->database_id};
    view.default_schema_id = SchemaId{prefix->default_schema_id};
    view.name = read_name(*prefix, tuple);
    return view;
}

std::optional<CatalogSchemaView> decode_catalog_schema(std::span<const std::byte> tuple)
{
    if (tuple.size() < sizeof(CatalogSchemaPrefix)) {
        return std::nullopt;
    }
    const auto* prefix = reinterpret_cast<const CatalogSchemaPrefix*>(tuple.data());
    CatalogSchemaView view{};
    view.tuple = prefix->tuple;
    view.schema_id = SchemaId{prefix->schema_id};
    view.database_id = DatabaseId{prefix->database_id};
    view.name = read_name(*prefix, tuple);
    return view;
}

std::optional<CatalogTableView> decode_catalog_table(std::span<const std::byte> tuple)
{
    if (tuple.size() < sizeof(CatalogTablePrefix)) {
        return std::nullopt;
    }
    const auto* prefix = reinterpret_cast<const CatalogTablePrefix*>(tuple.data());
    CatalogTableView view{};
    view.tuple = prefix->tuple;
    view.relation_id = RelationId{prefix->relation_id};
    view.schema_id = SchemaId{prefix->schema_id};
    view.table_type = static_cast<CatalogTableType>(prefix->table_type);
    view.root_page_id = prefix->root_page_id;
    view.name = read_name(*prefix, tuple);
    return view;
}

std::optional<CatalogColumnView> decode_catalog_column(std::span<const std::byte> tuple)
{
    if (tuple.size() < sizeof(CatalogColumnPrefix)) {
        return std::nullopt;
    }
    const auto* prefix = reinterpret_cast<const CatalogColumnPrefix*>(tuple.data());
    CatalogColumnView view{};
    view.tuple = prefix->tuple;
    view.column_id = ColumnId{prefix->column_id};
    view.relation_id = RelationId{prefix->relation_id};
    view.column_type = static_cast<CatalogColumnType>(prefix->column_type);
    view.ordinal_position = prefix->ordinal_position;
    view.name = read_name(*prefix, tuple);
    return view;
}

std::optional<CatalogIndexView> decode_catalog_index(std::span<const std::byte> tuple)
{
    if (tuple.size() < sizeof(CatalogIndexPrefix)) {
        return std::nullopt;
    }
    const auto* prefix = reinterpret_cast<const CatalogIndexPrefix*>(tuple.data());
    const auto prefix_size = sizeof(CatalogIndexPrefix);
    const auto total_length = static_cast<std::size_t>(prefix->name_length) + static_cast<std::size_t>(prefix->comparator_length);
    if (tuple.size() < prefix_size + total_length) {
        return std::nullopt;
    }
    CatalogIndexView view{};
    view.tuple = prefix->tuple;
    view.index_id = IndexId{prefix->index_id};
    view.relation_id = RelationId{prefix->relation_id};
    view.index_type = static_cast<CatalogIndexType>(prefix->index_type);
    view.max_fanout = prefix->max_fanout;
    view.root_page_id = prefix->root_page_id;
    if (prefix->name_length > 0U) {
        const auto* name_data = reinterpret_cast<const char*>(tuple.data() + prefix_size);
        view.name = {name_data, prefix->name_length};
    }
    if (prefix->comparator_length > 0U) {
        const auto* comparator_data = reinterpret_cast<const char*>(tuple.data() + prefix_size + prefix->name_length);
        view.comparator = {comparator_data, prefix->comparator_length};
    }
    return view;
}

}  // namespace bored::catalog
