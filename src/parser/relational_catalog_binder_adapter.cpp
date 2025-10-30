#include "bored/parser/relational/catalog_binder_adapter.hpp"

#include <algorithm>
#include <cctype>
#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace bored::parser::relational {
namespace {

std::string to_string(std::string_view view)
{
    return std::string(view.begin(), view.end());
}

}  // namespace

CatalogBinderAdapter::CatalogBinderAdapter(const catalog::CatalogAccessor& accessor) noexcept
    : accessor_{&accessor}
{}

std::string CatalogBinderAdapter::normalise(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    std::transform(text.begin(), text.end(), std::back_inserter(result), [](char ch) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    });
    return result;
}

std::optional<TableMetadata> CatalogBinderAdapter::lookup_table(std::optional<std::string_view> schema,
                                                               std::string_view table) const
{
    if (accessor_ == nullptr) {
        return std::nullopt;
    }

    const auto schemas = accessor_->schemas();
    if (schemas.empty()) {
        return std::nullopt;
    }

    std::unordered_map<std::uint64_t, catalog::CatalogSchemaDescriptor> schema_by_id;
    schema_by_id.reserve(schemas.size());
    std::unordered_map<std::string, std::vector<std::uint64_t>> schema_ids_by_name;
    for (const auto& descriptor : schemas) {
        schema_by_id.emplace(descriptor.schema_id.value, descriptor);
        schema_ids_by_name[normalise(descriptor.name)].push_back(descriptor.schema_id.value);
    }

    std::vector<std::uint64_t> candidate_schema_ids;
    if (schema.has_value()) {
        const auto key = normalise(*schema);
        auto it = schema_ids_by_name.find(key);
        if (it == schema_ids_by_name.end()) {
            return std::nullopt;
        }
        candidate_schema_ids = it->second;
    }

    const auto tables = accessor_->tables();
    const auto table_key = normalise(table);

    std::optional<TableMetadata> match{};

    for (const auto& descriptor : tables) {
        if (normalise(descriptor.name) != table_key) {
            continue;
        }

        auto schema_it = schema_by_id.find(descriptor.schema_id.value);
        if (schema_it == schema_by_id.end()) {
            continue;
        }

        if (schema.has_value()) {
            if (std::find(candidate_schema_ids.begin(), candidate_schema_ids.end(), descriptor.schema_id.value)
                == candidate_schema_ids.end()) {
                continue;
            }
        }

        TableMetadata metadata{};
        const auto& schema_descriptor = schema_it->second;
        metadata.database_id = schema_descriptor.database_id;
        metadata.schema_id = schema_descriptor.schema_id;
        metadata.relation_id = descriptor.relation_id;
        if (auto database_descriptor = accessor_->database(schema_descriptor.database_id)) {
            metadata.database_name = to_string(database_descriptor->name);
        }
        metadata.schema_name = to_string(schema_descriptor.name);
        metadata.table_name = to_string(descriptor.name);

        const auto columns = accessor_->columns(descriptor.relation_id);
        metadata.columns.reserve(columns.size());
        for (const auto& column : columns) {
            ColumnMetadata column_metadata{};
            column_metadata.column_id = column.column_id;
            column_metadata.column_type = column.column_type;
            column_metadata.name = to_string(column.name);
            metadata.columns.push_back(std::move(column_metadata));
        }

        if (match.has_value()) {
            return std::nullopt;
        }

        match = std::move(metadata);
    }

    return match;
}

}  // namespace bored::parser::relational
