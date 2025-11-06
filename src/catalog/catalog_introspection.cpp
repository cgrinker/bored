#include "bored/catalog/catalog_introspection.hpp"

#include "bored/catalog/catalog_accessor.hpp"

#include <algorithm>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace bored::catalog {
namespace {

CatalogIntrospectionSampler g_sampler{};
std::mutex g_sampler_mutex;

struct SchemaMetadata final {
    DatabaseId database_id{};
    std::string database_name{};
    std::string schema_name{};
};

[[nodiscard]] const char* to_string(CatalogRelationKind kind) noexcept
{
    switch (kind) {
    case CatalogRelationKind::Table:
        return "table";
    case CatalogRelationKind::SystemTable:
        return "system_table";
    case CatalogRelationKind::View:
    default:
        return "view";
    }
}

[[nodiscard]] const char* to_string(CatalogIndexType type) noexcept
{
    switch (type) {
    case CatalogIndexType::BTree:
        return "btree";
    case CatalogIndexType::Unknown:
    default:
        return "unknown";
    }
}

void append_json_string(std::string& out, const std::string& value)
{
    out.push_back('"');
    for (unsigned char ch : value) {
        switch (ch) {
        case '"':
            out.append("\\\"");
            break;
        case '\\':
            out.append("\\\\");
            break;
        case '\b':
            out.append("\\b");
            break;
        case '\f':
            out.append("\\f");
            break;
        case '\n':
            out.append("\\n");
            break;
        case '\r':
            out.append("\\r");
            break;
        case '\t':
            out.append("\\t");
            break;
        default:
            if (ch < 0x20U) {
                constexpr char kHex[] = "0123456789ABCDEF";
                out.append("\\u00");
                out.push_back(kHex[(ch >> 4U) & 0x0F]);
                out.push_back(kHex[ch & 0x0F]);
            } else {
                out.push_back(static_cast<char>(ch));
            }
            break;
        }
    }
    out.push_back('"');
}

}  // namespace

CatalogIntrospectionSnapshot collect_catalog_introspection(const CatalogAccessor& accessor)
{
    CatalogIntrospectionSnapshot snapshot{};

    std::unordered_map<std::uint64_t, SchemaMetadata> schema_metadata;

    const auto schemas = accessor.schemas();
    schema_metadata.reserve(schemas.size());
    for (const auto& schema : schemas) {
        SchemaMetadata metadata{};
        metadata.database_id = schema.database_id;
        metadata.schema_name.assign(schema.name.begin(), schema.name.end());
        if (auto database = accessor.database(schema.database_id); database) {
            metadata.database_name.assign(database->name.begin(), database->name.end());
        } else {
            metadata.database_name = "<unknown>";
        }
        schema_metadata.emplace(schema.schema_id.value, std::move(metadata));
    }

    struct RelationMetadata final {
        std::string schema_name;
        std::string relation_name;
    };

    std::unordered_map<std::uint64_t, RelationMetadata> relation_lookup;

    const auto tables = accessor.tables();
    snapshot.relations.reserve(tables.size());
    for (const auto& table : tables) {
        CatalogRelationSummary summary{};
        summary.relation_name.assign(table.name.begin(), table.name.end());
        summary.root_page_id = table.root_page_id;
        summary.column_count = static_cast<std::uint32_t>(accessor.columns(table.relation_id).size());

        const auto schema_it = schema_metadata.find(table.schema_id.value);
        if (schema_it != schema_metadata.end()) {
            summary.schema_name = schema_it->second.schema_name;
            summary.database_name = schema_it->second.database_name;
        } else {
            summary.schema_name = "<unknown>";
            summary.database_name = "<unknown>";
        }

        switch (table.table_type) {
        case CatalogTableType::Catalog:
            summary.relation_kind = CatalogRelationKind::SystemTable;
            break;
        case CatalogTableType::View:
            summary.relation_kind = CatalogRelationKind::View;
            break;
        case CatalogTableType::Heap:
        default:
            summary.relation_kind = CatalogRelationKind::Table;
            break;
        }

        relation_lookup.emplace(table.relation_id.value,
                                RelationMetadata{summary.schema_name, summary.relation_name});

        snapshot.relations.push_back(std::move(summary));
    }

    std::sort(snapshot.relations.begin(), snapshot.relations.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.database_name != rhs.database_name) {
            return lhs.database_name < rhs.database_name;
        }
        if (lhs.schema_name != rhs.schema_name) {
            return lhs.schema_name < rhs.schema_name;
        }
        return lhs.relation_name < rhs.relation_name;
    });

    const auto views = accessor.views();
    snapshot.views.reserve(views.size());
    for (const auto& view : views) {
        CatalogViewSummary summary{};
        summary.view_name.assign(view.name.begin(), view.name.end());
        summary.definition.assign(view.definition.begin(), view.definition.end());

        const auto schema_it = schema_metadata.find(view.schema_id.value);
        if (schema_it != schema_metadata.end()) {
            summary.schema_name = schema_it->second.schema_name;
            summary.database_name = schema_it->second.database_name;
        } else {
            summary.schema_name = "<unknown>";
            summary.database_name = "<unknown>";
        }

        snapshot.views.push_back(std::move(summary));
    }

    std::sort(snapshot.views.begin(), snapshot.views.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.database_name != rhs.database_name) {
            return lhs.database_name < rhs.database_name;
        }
        if (lhs.schema_name != rhs.schema_name) {
            return lhs.schema_name < rhs.schema_name;
        }
        return lhs.view_name < rhs.view_name;
    });

    for (const auto& table : tables) {
        const auto indexes = accessor.indexes(table.relation_id);
        for (const auto& index : indexes) {
            CatalogIndexSummary summary{};
            summary.index_name.assign(index.name.begin(), index.name.end());
            summary.index_type = index.index_type;
            summary.comparator.assign(index.comparator.begin(), index.comparator.end());
            summary.max_fanout = index.max_fanout;
            summary.root_page_id = index.root_page_id;

            const auto relation_it = relation_lookup.find(index.relation_id.value);
            if (relation_it != relation_lookup.end()) {
                summary.schema_name = relation_it->second.schema_name;
                summary.relation_name = relation_it->second.relation_name;
            } else {
                summary.schema_name = "<unknown>";
                summary.relation_name = "<unknown>";
            }

            snapshot.indexes.push_back(std::move(summary));
        }
    }

    std::sort(snapshot.indexes.begin(), snapshot.indexes.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.schema_name != rhs.schema_name) {
            return lhs.schema_name < rhs.schema_name;
        }
        if (lhs.relation_name != rhs.relation_name) {
            return lhs.relation_name < rhs.relation_name;
        }
        return lhs.index_name < rhs.index_name;
    });

    return snapshot;
}

std::string catalog_introspection_to_json(const CatalogIntrospectionSnapshot& snapshot)
{
    std::string json;
    json.reserve(1024U);
    json.push_back('{');

    auto append_field_name = [&json](const char* name, bool& first) {
        if (!first) {
            json.push_back(',');
        }
        first = false;
        json.push_back('"');
    json.append(name);
    json.append("\":");
    };

    bool first_field = true;
    append_field_name("schema_version", first_field);
    json.append(std::to_string(snapshot.schema_version));

    append_field_name("relations", first_field);
    json.push_back('[');
    for (std::size_t i = 0U; i < snapshot.relations.size(); ++i) {
        if (i > 0U) {
            json.push_back(',');
        }
        const auto& relation = snapshot.relations[i];
        json.push_back('{');
        bool first = true;
        auto append_string_field = [&](const char* name, const std::string& value) {
            if (!first) {
                json.push_back(',');
            }
            first = false;
            json.push_back('"');
            json.append(name);
            json.append("\":");
            append_json_string(json, value);
        };
        auto append_uint_field = [&](const char* name, std::uint64_t value) {
            if (!first) {
                json.push_back(',');
            }
            first = false;
            json.push_back('"');
            json.append(name);
            json.append("\":");
            json.append(std::to_string(value));
        };

        append_string_field("database", relation.database_name);
        append_string_field("schema", relation.schema_name);
        append_string_field("name", relation.relation_name);
        append_string_field("kind", to_string(relation.relation_kind));
        append_uint_field("root_page_id", relation.root_page_id);
        append_uint_field("column_count", relation.column_count);
        json.push_back('}');
    }
    json.push_back(']');

    append_field_name("indexes", first_field);
    json.push_back('[');
    for (std::size_t i = 0U; i < snapshot.indexes.size(); ++i) {
        if (i > 0U) {
            json.push_back(',');
        }
        const auto& index = snapshot.indexes[i];
        json.push_back('{');
        bool first = true;
        auto append_string_field = [&](const char* name, const std::string& value) {
            if (!first) {
                json.push_back(',');
            }
            first = false;
            json.push_back('"');
            json.append(name);
            json.append("\":");
            append_json_string(json, value);
        };
        auto append_uint_field = [&](const char* name, std::uint64_t value) {
            if (!first) {
                json.push_back(',');
            }
            first = false;
            json.push_back('"');
            json.append(name);
            json.append("\":");
            json.append(std::to_string(value));
        };

        append_string_field("schema", index.schema_name);
        append_string_field("relation", index.relation_name);
        append_string_field("name", index.index_name);
        append_string_field("type", to_string(index.index_type));
        append_string_field("comparator", index.comparator);
        append_uint_field("max_fanout", index.max_fanout);
        append_uint_field("root_page_id", index.root_page_id);
        json.push_back('}');
    }
    json.push_back(']');

    append_field_name("views", first_field);
    json.push_back('[');
    for (std::size_t i = 0U; i < snapshot.views.size(); ++i) {
        if (i > 0U) {
            json.push_back(',');
        }
        const auto& view = snapshot.views[i];
        json.push_back('{');
        bool first = true;
        auto append_string_field = [&](const char* name, const std::string& value) {
            if (!first) {
                json.push_back(',');
            }
            first = false;
            json.push_back('"');
            json.append(name);
            json.append("\":");
            append_json_string(json, value);
        };

        append_string_field("database", view.database_name);
        append_string_field("schema", view.schema_name);
        append_string_field("name", view.view_name);
        append_string_field("definition", view.definition);
        json.push_back('}');
    }
    json.push_back(']');

    json.push_back('}');
    return json;
}

void set_global_catalog_introspection_sampler(CatalogIntrospectionSampler sampler) noexcept
{
    std::scoped_lock lock{g_sampler_mutex};
    g_sampler = std::move(sampler);
}

CatalogIntrospectionSampler get_global_catalog_introspection_sampler() noexcept
{
    std::scoped_lock lock{g_sampler_mutex};
    return g_sampler;
}

CatalogIntrospectionSnapshot collect_global_catalog_introspection()
{
    auto sampler = get_global_catalog_introspection_sampler();
    if (sampler) {
        return sampler();
    }
    return CatalogIntrospectionSnapshot{};
}

}  // namespace bored::catalog
