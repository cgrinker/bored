#pragma once

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/parser/relational/binder.hpp"

#include <optional>
#include <string>
#include <string_view>

namespace bored::parser::relational {

class CatalogBinderAdapter final : public BinderCatalog {
public:
    explicit CatalogBinderAdapter(const catalog::CatalogAccessor& accessor) noexcept;

    std::optional<TableMetadata> lookup_table(std::optional<std::string_view> schema,
                                              std::string_view table) const override;

private:
    static std::string normalise(std::string_view text);

    const catalog::CatalogAccessor* accessor_ = nullptr;
};

}  // namespace bored::parser::relational
