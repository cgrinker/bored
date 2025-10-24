#pragma once

#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_encoding.hpp"

#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_manager.hpp"

#include <array>
#include <cstdint>
#include <span>
#include <system_error>
#include <unordered_map>

namespace bored::catalog {

struct CatalogBootstrapArtifacts final {
    std::unordered_map<std::uint32_t, std::array<std::byte, bored::storage::kPageSize>> pages;
};

struct CatalogBootstrapConfig final {
    bored::storage::PageManager* page_manager = nullptr;
    bored::storage::FreeSpaceMap* free_space_map = nullptr;
    bool flush_wal = true;
};

class CatalogBootstrapper final {
public:
    explicit CatalogBootstrapper(CatalogBootstrapConfig config);

    [[nodiscard]] std::error_code run(CatalogBootstrapArtifacts& artifacts) const;

private:
    CatalogBootstrapConfig config_{};

    [[nodiscard]] std::error_code bootstrap_databases(std::span<std::byte> page) const;
    [[nodiscard]] std::error_code bootstrap_schemas(std::span<std::byte> page) const;
    [[nodiscard]] std::error_code bootstrap_tables(std::span<std::byte> page) const;
    [[nodiscard]] std::error_code bootstrap_columns(std::span<std::byte> page) const;
    [[nodiscard]] std::error_code bootstrap_indexes(std::span<std::byte> page) const;
};

}  // namespace bored::catalog
