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

struct CatalogConstraintPrefix final {
    CatalogTupleDescriptor tuple{};
    std::uint64_t constraint_id = 0U;
    std::uint64_t relation_id = 0U;
    std::uint64_t backing_index_id = 0U;
    std::uint64_t referenced_relation_id = 0U;
    std::uint16_t constraint_type = 0U;
    std::uint16_t name_length = 0U;
    std::uint16_t key_columns_length = 0U;
    std::uint16_t referenced_columns_length = 0U;
    std::uint32_t padding = 0U;
};

struct CatalogSequencePrefix final {
    CatalogTupleDescriptor tuple{};
    std::uint64_t sequence_id = 0U;
    std::uint64_t schema_id = 0U;
    std::uint64_t owning_relation_id = 0U;
    std::uint64_t owning_column_id = 0U;
    std::int64_t increment = 1;
    std::uint64_t start_value = 1U;
    std::uint64_t next_value = 1U;
    std::uint64_t min_value = 1U;
    std::uint64_t max_value = 0U;
    std::uint64_t cache_size = 1U;
    std::uint16_t flags = 0U;
    std::uint16_t name_length = 0U;
    std::uint32_t padding = 0U;
};

constexpr std::uint16_t kSequenceCycleFlag = 0x1U;

static_assert(sizeof(CatalogDatabasePrefix) == 48U, "CatalogDatabasePrefix expected to be 48 bytes");
static_assert(sizeof(CatalogSchemaPrefix) == 48U, "CatalogSchemaPrefix expected to be 48 bytes");
static_assert(sizeof(CatalogTablePrefix) == 56U, "CatalogTablePrefix expected to be 56 bytes");
static_assert(sizeof(CatalogColumnPrefix) == 56U, "CatalogColumnPrefix expected to be 56 bytes");
static_assert(sizeof(CatalogIndexPrefix) == 56U, "CatalogIndexPrefix expected to be 56 bytes");
static_assert(sizeof(CatalogConstraintPrefix) == 72U, "CatalogConstraintPrefix expected to be 72 bytes");
static_assert(sizeof(CatalogSequencePrefix) == 112U, "CatalogSequencePrefix expected to be 112 bytes");

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

std::size_t catalog_constraint_tuple_size(std::string_view name,
                                          std::string_view key_columns,
                                          std::string_view referenced_columns) noexcept
{
    return align8(sizeof(CatalogConstraintPrefix) + name.size() + key_columns.size() + referenced_columns.size());
}

std::size_t catalog_sequence_tuple_size(std::string_view name) noexcept
{
    return align8(sizeof(CatalogSequencePrefix) + name.size());
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

std::vector<std::byte> serialize_catalog_constraint(const CatalogConstraintDescriptor& descriptor)
{
    const auto name_length = static_cast<std::uint16_t>(descriptor.name.size());
    const auto key_length = static_cast<std::uint16_t>(descriptor.key_columns.size());
    const auto ref_length = static_cast<std::uint16_t>(descriptor.referenced_columns.size());
    const auto prefix_size = sizeof(CatalogConstraintPrefix);
    auto buffer = std::vector<std::byte>(align8(prefix_size + descriptor.name.size() + descriptor.key_columns.size() + descriptor.referenced_columns.size()), std::byte{0});
    auto* prefix = reinterpret_cast<CatalogConstraintPrefix*>(buffer.data());
    prefix->tuple = descriptor.tuple;
    prefix->constraint_id = descriptor.constraint_id.value;
    prefix->relation_id = descriptor.relation_id.value;
    prefix->backing_index_id = descriptor.backing_index_id.value;
    prefix->referenced_relation_id = descriptor.referenced_relation_id.value;
    prefix->constraint_type = static_cast<std::uint16_t>(descriptor.constraint_type);
    prefix->name_length = name_length;
    prefix->key_columns_length = key_length;
    prefix->referenced_columns_length = ref_length;
    auto offset = prefix_size;
    if (name_length > 0U) {
        std::memcpy(buffer.data() + offset, descriptor.name.data(), descriptor.name.size());
        offset += descriptor.name.size();
    }
    if (key_length > 0U) {
        std::memcpy(buffer.data() + offset, descriptor.key_columns.data(), descriptor.key_columns.size());
        offset += descriptor.key_columns.size();
    }
    if (ref_length > 0U) {
        std::memcpy(buffer.data() + offset, descriptor.referenced_columns.data(), descriptor.referenced_columns.size());
    }
    return buffer;
}

std::vector<std::byte> serialize_catalog_sequence(const CatalogSequenceDescriptor& descriptor)
{
    auto buffer = serialize_prefixed_tuple(sizeof(CatalogSequencePrefix), descriptor.name);
    auto* prefix = reinterpret_cast<CatalogSequencePrefix*>(buffer.data());
    prefix->tuple = descriptor.tuple;
    prefix->sequence_id = descriptor.sequence_id.value;
    prefix->schema_id = descriptor.schema_id.value;
    prefix->owning_relation_id = descriptor.owning_relation_id.value;
    prefix->owning_column_id = descriptor.owning_column_id.value;
    prefix->increment = descriptor.increment;
    prefix->start_value = descriptor.start_value;
    prefix->next_value = descriptor.next_value;
    prefix->min_value = descriptor.min_value;
    prefix->max_value = descriptor.max_value;
    prefix->cache_size = descriptor.cache_size;
    prefix->flags = descriptor.cycle ? kSequenceCycleFlag : 0U;
    prefix->name_length = static_cast<std::uint16_t>(descriptor.name.size());
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

std::optional<CatalogConstraintView> decode_catalog_constraint(std::span<const std::byte> tuple)
{
    if (tuple.size() < sizeof(CatalogConstraintPrefix)) {
        return std::nullopt;
    }
    const auto* prefix = reinterpret_cast<const CatalogConstraintPrefix*>(tuple.data());
    const auto prefix_size = sizeof(CatalogConstraintPrefix);
    const auto total_length = static_cast<std::size_t>(prefix->name_length) +
                              static_cast<std::size_t>(prefix->key_columns_length) +
                              static_cast<std::size_t>(prefix->referenced_columns_length);
    if (tuple.size() < prefix_size + total_length) {
        return std::nullopt;
    }
    CatalogConstraintView view{};
    view.tuple = prefix->tuple;
    view.constraint_id = ConstraintId{prefix->constraint_id};
    view.relation_id = RelationId{prefix->relation_id};
    view.constraint_type = static_cast<CatalogConstraintType>(prefix->constraint_type);
    view.backing_index_id = IndexId{prefix->backing_index_id};
    view.referenced_relation_id = RelationId{prefix->referenced_relation_id};
    const char* base = reinterpret_cast<const char*>(tuple.data() + prefix_size);
    if (prefix->name_length > 0U) {
        view.name = {base, prefix->name_length};
    }
    base += prefix->name_length;
    if (prefix->key_columns_length > 0U) {
        view.key_columns = {base, prefix->key_columns_length};
    }
    base += prefix->key_columns_length;
    if (prefix->referenced_columns_length > 0U) {
        view.referenced_columns = {base, prefix->referenced_columns_length};
    }
    return view;
}

std::optional<CatalogSequenceView> decode_catalog_sequence(std::span<const std::byte> tuple)
{
    if (tuple.size() < sizeof(CatalogSequencePrefix)) {
        return std::nullopt;
    }
    const auto* prefix = reinterpret_cast<const CatalogSequencePrefix*>(tuple.data());
    CatalogSequenceView view{};
    view.tuple = prefix->tuple;
    view.sequence_id = SequenceId{prefix->sequence_id};
    view.schema_id = SchemaId{prefix->schema_id};
    view.owning_relation_id = RelationId{prefix->owning_relation_id};
    view.owning_column_id = ColumnId{prefix->owning_column_id};
    view.increment = prefix->increment;
    view.start_value = prefix->start_value;
    view.next_value = prefix->next_value;
    view.min_value = prefix->min_value;
    view.max_value = prefix->max_value;
    view.cache_size = prefix->cache_size;
    view.cycle = (prefix->flags & kSequenceCycleFlag) != 0U;
    view.name = read_name(*prefix, tuple);
    return view;
}

}  // namespace bored::catalog
