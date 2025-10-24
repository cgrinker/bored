#include "bored/catalog/catalog_mutator.hpp"

#include <stdexcept>
#include <utility>

namespace bored::catalog {

namespace {

[[nodiscard]] CatalogTupleVersion make_version(CatalogTupleDescriptor descriptor, std::vector<std::byte> payload)
{
    CatalogTupleVersion version{};
    version.descriptor = descriptor;
    version.payload = std::move(payload);
    return version;
}

}  // namespace

CatalogMutator::CatalogMutator(CatalogMutatorConfig config)
    : transaction_{config.transaction}
{
    if (transaction_ == nullptr) {
        throw std::invalid_argument{"CatalogMutator requires an active transaction"};
    }
}

const CatalogTransaction& CatalogMutator::transaction() const noexcept
{
    return *transaction_;
}

bool CatalogMutator::empty() const noexcept
{
    return staged_.empty();
}

const std::vector<CatalogStagedMutation>& CatalogMutator::staged_mutations() const noexcept
{
    return staged_;
}

const std::vector<std::optional<CatalogWalRecordStaging>>& CatalogMutator::staged_wal_records() const noexcept
{
    return wal_records_;
}

void CatalogMutator::stage_insert(RelationId relation_id,
                                  std::uint64_t row_id,
                                  CatalogTupleDescriptor descriptor,
                                  std::vector<std::byte> payload)
{
    if (row_id == 0U) {
        throw std::invalid_argument{"CatalogMutator::stage_insert requires non-zero row id"};
    }

    CatalogStagedMutation mutation{};
    mutation.kind = CatalogMutationKind::Insert;
    mutation.relation_id = relation_id;
    mutation.row_id = row_id;
    mutation.after = make_version(descriptor, std::move(payload));

    staged_.push_back(std::move(mutation));
    wal_records_.emplace_back(std::nullopt);
}

void CatalogMutator::stage_delete(RelationId relation_id,
                                  std::uint64_t row_id,
                                  const CatalogTupleDescriptor& existing_descriptor,
                                  std::vector<std::byte> payload)
{
    if (row_id == 0U) {
        throw std::invalid_argument{"CatalogMutator::stage_delete requires non-zero row id"};
    }

    CatalogStagedMutation mutation{};
    mutation.kind = CatalogMutationKind::Delete;
    mutation.relation_id = relation_id;
    mutation.row_id = row_id;
    mutation.before = make_version(existing_descriptor, std::move(payload));

    staged_.push_back(std::move(mutation));
    wal_records_.emplace_back(std::nullopt);
}

void CatalogMutator::stage_update(RelationId relation_id,
                                  std::uint64_t row_id,
                                  const CatalogTupleDescriptor& existing_descriptor,
                                  std::vector<std::byte> before_payload,
                                  CatalogTupleDescriptor updated_descriptor,
                                  std::vector<std::byte> after_payload)
{
    if (row_id == 0U) {
        throw std::invalid_argument{"CatalogMutator::stage_update requires non-zero row id"};
    }

    CatalogStagedMutation mutation{};
    mutation.kind = CatalogMutationKind::Update;
    mutation.relation_id = relation_id;
    mutation.row_id = row_id;
    mutation.before = make_version(existing_descriptor, std::move(before_payload));
    mutation.after = make_version(updated_descriptor, std::move(after_payload));

    staged_.push_back(std::move(mutation));
    wal_records_.emplace_back(std::nullopt);
}

void CatalogMutator::clear() noexcept
{
    staged_.clear();
    wal_records_.clear();
}

CatalogWalRecordStaging& CatalogMutator::ensure_wal_record(std::size_t index)
{
    if (index >= wal_records_.size()) {
        throw std::out_of_range{"CatalogMutator wal record index out of range"};
    }
    auto& entry = wal_records_[index];
    if (!entry.has_value()) {
        entry.emplace();
    }
    return *entry;
}

const std::optional<CatalogWalRecordStaging>& CatalogMutator::wal_record(std::size_t index) const
{
    if (index >= wal_records_.size()) {
        throw std::out_of_range{"CatalogMutator wal record index out of range"};
    }
    return wal_records_[index];
}

void CatalogMutator::clear_wal_record(std::size_t index) noexcept
{
    if (index < wal_records_.size()) {
        wal_records_[index].reset();
    }
}

CatalogTupleDescriptor CatalogTupleBuilder::for_insert(const CatalogTransaction& transaction,
                                                       std::uint32_t visibility_flags) noexcept
{
    CatalogTupleDescriptor descriptor{};
    descriptor.xmin = transaction.transaction_id();
    descriptor.xmax = 0U;
    descriptor.visibility_flags = visibility_flags;
    return descriptor;
}

CatalogTupleDescriptor CatalogTupleBuilder::for_update(const CatalogTransaction& transaction,
                                                       const CatalogTupleDescriptor& existing,
                                                       std::uint32_t visibility_flags) noexcept
{
    CatalogTupleDescriptor descriptor = existing;
    descriptor.xmin = transaction.transaction_id();
    descriptor.xmax = 0U;
    descriptor.visibility_flags = visibility_flags;
    return descriptor;
}

CatalogTupleDescriptor CatalogTupleBuilder::for_delete(const CatalogTransaction& transaction,
                                                       const CatalogTupleDescriptor& existing) noexcept
{
    CatalogTupleDescriptor descriptor = existing;
    descriptor.xmax = transaction.transaction_id();
    return descriptor;
}

}  // namespace bored::catalog
