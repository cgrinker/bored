#include "bored/catalog/catalog_mutator.hpp"

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_pages.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <array>
#include <atomic>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <span>
#include <utility>

namespace bored::catalog {

namespace {

std::atomic<std::uint64_t> g_published_batches{0U};
std::atomic<std::uint64_t> g_published_mutations{0U};
std::atomic<std::uint64_t> g_published_wal_records{0U};
std::atomic<std::uint64_t> g_publish_failures{0U};
std::atomic<std::uint64_t> g_aborted_batches{0U};
std::atomic<std::uint64_t> g_aborted_mutations{0U};

[[nodiscard]] CatalogTupleVersion make_version(CatalogTupleDescriptor descriptor, std::vector<std::byte> payload)
{
    CatalogTupleVersion version{};
    version.descriptor = descriptor;
    version.payload = std::move(payload);
    return version;
}

[[nodiscard]] std::span<const std::byte> as_const_span(const std::vector<std::byte>& buffer) noexcept
{
    return {buffer.data(), buffer.size()};
}

[[nodiscard]] bool payload_fits(std::size_t length) noexcept
{
    return length <= std::numeric_limits<std::uint16_t>::max();
}

std::error_code append_before_image(const CatalogTupleVersion& before,
                                    std::uint64_t row_id,
                                    std::uint32_t page_id,
                                    CatalogWalRecordStaging& staging)
{
    if (!payload_fits(before.payload.size())) {
        return std::make_error_code(std::errc::value_too_large);
    }

    storage::WalTupleMeta meta{};
    meta.page_id = page_id;
    meta.slot_index = 0U;
    meta.tuple_length = static_cast<std::uint16_t>(before.payload.size());
    meta.row_id = row_id;

    const auto before_size = storage::wal_tuple_before_image_payload_size(meta.tuple_length,
                                                                          std::span<const storage::WalOverflowChunkMeta>{});
    std::vector<std::byte> buffer(before_size);
    auto buffer_span = std::span<std::byte>(buffer.data(), buffer.size());
    std::array<std::span<const std::byte>, 0> empty_chunks{};
    auto chunk_payload_span = std::span<const std::span<const std::byte>>(empty_chunks.data(), empty_chunks.size());
    if (!storage::encode_wal_tuple_before_image(buffer_span,
                                               meta,
                                               as_const_span(before.payload),
                                               std::span<const storage::WalOverflowChunkMeta>{},
                                               chunk_payload_span)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    CatalogWalRecordFragment fragment{};
    fragment.type = storage::WalRecordType::TupleBeforeImage;
    fragment.flags = storage::WalRecordFlag::HasPayload;
    fragment.page_id = page_id;
    fragment.payload = std::move(buffer);
    staging.records.push_back(std::move(fragment));
    return {};
}

std::error_code append_insert_record(const CatalogStagedMutation& mutation,
                                     std::uint32_t page_id,
                                     CatalogWalRecordStaging& staging)
{
    if (!mutation.after) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    const auto& after = *mutation.after;

    if (!payload_fits(after.payload.size())) {
        return std::make_error_code(std::errc::value_too_large);
    }

    storage::WalTupleMeta meta{};
    meta.page_id = page_id;
    meta.slot_index = 0U;
    meta.tuple_length = static_cast<std::uint16_t>(after.payload.size());
    meta.row_id = mutation.row_id;

    std::vector<std::byte> buffer(storage::wal_tuple_insert_payload_size(meta.tuple_length));
    auto buffer_span = std::span<std::byte>(buffer.data(), buffer.size());
    if (!storage::encode_wal_tuple_insert(buffer_span, meta, as_const_span(after.payload))) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    CatalogWalRecordFragment fragment{};
    fragment.type = storage::WalRecordType::CatalogInsert;
    fragment.flags = storage::WalRecordFlag::HasPayload;
    fragment.page_id = page_id;
    fragment.payload = std::move(buffer);
    staging.records.push_back(std::move(fragment));
    return {};
}

std::error_code append_delete_record(const CatalogStagedMutation& mutation,
                                     std::uint32_t page_id,
                                     CatalogWalRecordStaging& staging)
{
    storage::WalTupleMeta meta{};
    meta.page_id = page_id;
    meta.slot_index = 0U;
    meta.tuple_length = 0U;
    meta.row_id = mutation.row_id;

    std::vector<std::byte> buffer(storage::wal_tuple_delete_payload_size());
    auto buffer_span = std::span<std::byte>(buffer.data(), buffer.size());
    if (!storage::encode_wal_tuple_delete(buffer_span, meta)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    CatalogWalRecordFragment fragment{};
    fragment.type = storage::WalRecordType::CatalogDelete;
    fragment.flags = storage::WalRecordFlag::HasPayload;
    fragment.page_id = page_id;
    fragment.payload = std::move(buffer);
    staging.records.push_back(std::move(fragment));
    return {};
}

std::error_code append_update_record(const CatalogStagedMutation& mutation,
                                     std::uint32_t page_id,
                                     CatalogWalRecordStaging& staging)
{
    if (!mutation.before || !mutation.after) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    const auto& before = *mutation.before;
    const auto& after = *mutation.after;

    if (!payload_fits(before.payload.size()) || !payload_fits(after.payload.size())) {
        return std::make_error_code(std::errc::value_too_large);
    }

    storage::WalTupleUpdateMeta meta{};
    meta.base.page_id = page_id;
    meta.base.slot_index = 0U;
    meta.base.tuple_length = static_cast<std::uint16_t>(after.payload.size());
    meta.base.row_id = mutation.row_id;
    meta.old_length = static_cast<std::uint16_t>(before.payload.size());

    std::vector<std::byte> buffer(storage::wal_tuple_update_payload_size(meta.base.tuple_length));
    auto buffer_span = std::span<std::byte>(buffer.data(), buffer.size());
    if (!storage::encode_wal_tuple_update(buffer_span, meta, as_const_span(after.payload))) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    CatalogWalRecordFragment fragment{};
    fragment.type = storage::WalRecordType::CatalogUpdate;
    fragment.flags = storage::WalRecordFlag::HasPayload;
    fragment.page_id = page_id;
    fragment.payload = std::move(buffer);
    staging.records.push_back(std::move(fragment));
    return {};
}

std::error_code build_wal_records_for_mutation(const CatalogStagedMutation& mutation,
                                               std::uint32_t page_id,
                                               CatalogWalRecordStaging& staging)
{
    staging.records.clear();

    switch (mutation.kind) {
    case CatalogMutationKind::Insert:
        return append_insert_record(mutation, page_id, staging);
    case CatalogMutationKind::Delete:
        if (!mutation.before) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (auto ec = append_before_image(*mutation.before, mutation.row_id, page_id, staging); ec) {
            return ec;
        }
        return append_delete_record(mutation, page_id, staging);
    case CatalogMutationKind::Update:
        if (!mutation.before || !mutation.after) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (auto ec = append_before_image(*mutation.before, mutation.row_id, page_id, staging); ec) {
            return ec;
        }
        return append_update_record(mutation, page_id, staging);
    }

    return std::make_error_code(std::errc::invalid_argument);
}

}  // namespace

CatalogMutator::CatalogMutator(CatalogMutatorConfig config)
    : transaction_{config.transaction}
    , commit_lsn_provider_{std::move(config.commit_lsn_provider)}
{
    if (transaction_ == nullptr) {
        throw std::invalid_argument{"CatalogMutator requires an active transaction"};
    }
    register_transaction_hooks();
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

bool CatalogMutator::has_published_batch() const noexcept
{
    return published_batch_.has_value();
}

const CatalogMutationBatch& CatalogMutator::published_batch() const
{
    if (!published_batch_) {
        throw std::logic_error{"CatalogMutator::published_batch called without published batch"};
    }
    return *published_batch_;
}

CatalogMutationBatch CatalogMutator::consume_published_batch()
{
    if (!published_batch_) {
        throw std::logic_error{"CatalogMutator::consume_published_batch called without published batch"};
    }
    auto batch = std::move(*published_batch_);
    published_batch_.reset();
    return batch;
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
    published_batch_.reset();
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

void CatalogMutator::set_publish_listener(PublishListener listener)
{
    publish_listener_ = std::move(listener);
}

std::error_code CatalogMutator::publish_staged_batch()
{
    if (staged_.empty()) {
        published_batch_.reset();
        wal_records_.clear();
        return {};
    }

    for (std::size_t index = 0; index < staged_.size(); ++index) {
        const auto page_id_opt = catalog_relation_page(staged_[index].relation_id);
        if (!page_id_opt) {
            g_publish_failures.fetch_add(1U, std::memory_order_relaxed);
            return std::make_error_code(std::errc::invalid_argument);
        }

        auto& wal_entry = ensure_wal_record(index);
        wal_entry.records.clear();

        if (auto ec = build_wal_records_for_mutation(staged_[index], *page_id_opt, wal_entry); ec) {
            g_publish_failures.fetch_add(1U, std::memory_order_relaxed);
            return ec;
        }
    }

    CatalogMutationBatch batch{};
    batch.mutations = std::move(staged_);
    batch.wal_records = std::move(wal_records_);
    batch.commit_lsn = commit_lsn_provider_ ? commit_lsn_provider_() : 0U;
    for (auto& entry : batch.wal_records) {
        if (entry) {
            entry->commit_lsn = batch.commit_lsn;
        }
    }

    std::uint64_t wal_record_count = 0U;
    for (const auto& entry : batch.wal_records) {
        if (entry) {
            wal_record_count += entry->records.size();
        }
    }

    g_published_batches.fetch_add(1U, std::memory_order_relaxed);
    g_published_mutations.fetch_add(batch.mutations.size(), std::memory_order_relaxed);
    g_published_wal_records.fetch_add(wal_record_count, std::memory_order_relaxed);

    if (publish_listener_) {
        if (auto ec = publish_listener_(batch)) {
            g_publish_failures.fetch_add(1U, std::memory_order_relaxed);
            return ec;
        }
    }

    std::array<RelationId, 5U> mutated_relations{};
    std::size_t mutated_count = 0U;
    auto record_relation = [&mutated_relations, &mutated_count](RelationId relation_id) {
        if (!relation_id.is_valid()) {
            return;
        }
        for (std::size_t index = 0U; index < mutated_count; ++index) {
            if (mutated_relations[index] == relation_id) {
                return;
            }
        }
        mutated_relations[mutated_count++] = relation_id;
    };

    for (const auto& mutation : batch.mutations) {
        record_relation(mutation.relation_id);
    }

    for (std::size_t index = 0U; index < mutated_count; ++index) {
        CatalogAccessor::invalidate_relation(mutated_relations[index]);
    }

    published_batch_ = std::move(batch);
    staged_.clear();
    wal_records_.clear();
    return {};
}

void CatalogMutator::register_transaction_hooks()
{
    transaction_->register_commit_hook([this]() {
        return this->publish_staged_batch();
    });
    transaction_->register_abort_hook([this]() {
        this->record_abort();
        this->clear();
    });
}

CatalogMutationTelemetrySnapshot CatalogMutator::telemetry() noexcept
{
    CatalogMutationTelemetrySnapshot snapshot{};
    snapshot.published_batches = g_published_batches.load(std::memory_order_relaxed);
    snapshot.published_mutations = g_published_mutations.load(std::memory_order_relaxed);
    snapshot.published_wal_records = g_published_wal_records.load(std::memory_order_relaxed);
    snapshot.publish_failures = g_publish_failures.load(std::memory_order_relaxed);
    snapshot.aborted_batches = g_aborted_batches.load(std::memory_order_relaxed);
    snapshot.aborted_mutations = g_aborted_mutations.load(std::memory_order_relaxed);
    return snapshot;
}

void CatalogMutator::reset_telemetry() noexcept
{
    g_published_batches.store(0U, std::memory_order_relaxed);
    g_published_mutations.store(0U, std::memory_order_relaxed);
    g_published_wal_records.store(0U, std::memory_order_relaxed);
    g_publish_failures.store(0U, std::memory_order_relaxed);
    g_aborted_batches.store(0U, std::memory_order_relaxed);
    g_aborted_mutations.store(0U, std::memory_order_relaxed);
}

void CatalogMutator::record_abort() noexcept
{
    if (staged_.empty()) {
        return;
    }
    g_aborted_batches.fetch_add(1U, std::memory_order_relaxed);
    g_aborted_mutations.fetch_add(staged_.size(), std::memory_order_relaxed);
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
