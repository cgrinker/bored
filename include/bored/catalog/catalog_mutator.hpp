#pragma once

#include "bored/catalog/catalog_relations.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/storage/wal_format.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>
#include <vector>

namespace bored::catalog {

struct CatalogTupleVersion final {
    CatalogTupleDescriptor descriptor{};
    std::vector<std::byte> payload{};
};

enum class CatalogMutationKind {
    Insert,
    Update,
    Delete
};

struct CatalogWalRecordStaging final {
    storage::WalRecordType type = storage::WalRecordType::CatalogInsert;
    storage::WalRecordFlag flags = storage::WalRecordFlag::None;
    std::uint32_t page_id = 0U;
    std::vector<std::byte> payload{};
};

struct CatalogStagedMutation final {
    CatalogMutationKind kind = CatalogMutationKind::Insert;
    RelationId relation_id{};
    std::uint64_t row_id = 0U;
    std::optional<CatalogTupleVersion> before{};
    std::optional<CatalogTupleVersion> after{};
};

struct CatalogMutatorConfig final {
    CatalogTransaction* transaction = nullptr;
};

struct CatalogMutationBatch final {
    std::vector<CatalogStagedMutation> mutations{};
    std::vector<std::optional<CatalogWalRecordStaging>> wal_records{};
};

class CatalogMutator final {
public:
    explicit CatalogMutator(CatalogMutatorConfig config);

    CatalogMutator(const CatalogMutator&) = delete;
    CatalogMutator& operator=(const CatalogMutator&) = delete;
    CatalogMutator(CatalogMutator&&) = delete;
    CatalogMutator& operator=(CatalogMutator&&) = delete;

    [[nodiscard]] const CatalogTransaction& transaction() const noexcept;
    [[nodiscard]] bool empty() const noexcept;
    [[nodiscard]] const std::vector<CatalogStagedMutation>& staged_mutations() const noexcept;
    [[nodiscard]] const std::vector<std::optional<CatalogWalRecordStaging>>& staged_wal_records() const noexcept;
    [[nodiscard]] bool has_published_batch() const noexcept;
    [[nodiscard]] const CatalogMutationBatch& published_batch() const;
    CatalogMutationBatch consume_published_batch();

    void stage_insert(RelationId relation_id,
                      std::uint64_t row_id,
                      CatalogTupleDescriptor descriptor,
                      std::vector<std::byte> payload);

    void stage_delete(RelationId relation_id,
                      std::uint64_t row_id,
                      const CatalogTupleDescriptor& existing_descriptor,
                      std::vector<std::byte> payload);

    void stage_update(RelationId relation_id,
                      std::uint64_t row_id,
                      const CatalogTupleDescriptor& existing_descriptor,
                      std::vector<std::byte> before_payload,
                      CatalogTupleDescriptor updated_descriptor,
                      std::vector<std::byte> after_payload);

    void clear() noexcept;

    CatalogWalRecordStaging& ensure_wal_record(std::size_t index);
    [[nodiscard]] const std::optional<CatalogWalRecordStaging>& wal_record(std::size_t index) const;
    void clear_wal_record(std::size_t index) noexcept;

private:
    std::error_code publish_staged_batch();
    void register_transaction_hooks();

    CatalogTransaction* transaction_ = nullptr;
    std::vector<CatalogStagedMutation> staged_{};
    std::vector<std::optional<CatalogWalRecordStaging>> wal_records_{};
    std::optional<CatalogMutationBatch> published_batch_{};
};

struct CatalogTupleBuilder final {
    static CatalogTupleDescriptor for_insert(const CatalogTransaction& transaction,
                                             std::uint32_t visibility_flags = 0U) noexcept;

    static CatalogTupleDescriptor for_update(const CatalogTransaction& transaction,
                                             const CatalogTupleDescriptor& existing,
                                             std::uint32_t visibility_flags = 0U) noexcept;

    static CatalogTupleDescriptor for_delete(const CatalogTransaction& transaction,
                                             const CatalogTupleDescriptor& existing) noexcept;
};

}  // namespace bored::catalog
