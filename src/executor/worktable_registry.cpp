#include "bored/executor/worktable_registry.hpp"

#include <utility>

namespace bored::executor {

WorkTableRegistry::SnapshotIterator::SnapshotIterator(txn::Snapshot snapshot,
                                                      std::shared_ptr<const std::vector<TupleBuffer>> rows) noexcept
    : snapshot_{snapshot}
    , rows_{std::move(rows)}
{
}

WorkTableRegistry::RecursiveCursor::RecursiveCursor(txn::Snapshot snapshot,
                                                    std::shared_ptr<std::vector<TupleBuffer>> rows,
                                                    std::size_t seed_count) noexcept
    : snapshot_{snapshot}
    , rows_{std::move(rows)}
    , seed_count_{seed_count}
    , seed_index_{0U}
    , delta_begin_{seed_count_}
    , delta_index_{seed_count_}
{
}

bool WorkTableRegistry::SnapshotIterator::matches(const txn::Snapshot& snapshot) const noexcept
{
    return txn::snapshots_equal(snapshot_, snapshot);
}

std::size_t WorkTableRegistry::SnapshotIterator::size() const noexcept
{
    return rows_ ? rows_->size() : 0U;
}

bool WorkTableRegistry::SnapshotIterator::next(const TupleBuffer*& tuple) noexcept
{
    if (!rows_ || index_ >= rows_->size()) {
        tuple = nullptr;
        return false;
    }

    tuple = &(*rows_)[index_++];
    return true;
}

std::span<const TupleBuffer> WorkTableRegistry::SnapshotIterator::span() const noexcept
{
    if (!rows_) {
        return {};
    }
    return std::span<const TupleBuffer>(rows_->data(), rows_->size());
}

bool WorkTableRegistry::RecursiveCursor::next_seed(const TupleBuffer*& tuple) noexcept
{
    if (!rows_ || seed_index_ >= seed_count_) {
        tuple = nullptr;
        return false;
    }
    tuple = &(*rows_)[seed_index_++];
    return true;
}

void WorkTableRegistry::RecursiveCursor::append_delta(TupleBuffer row)
{
    if (!rows_) {
        return;
    }
    rows_->push_back(std::move(row));
}

bool WorkTableRegistry::RecursiveCursor::next_delta(const TupleBuffer*& tuple) noexcept
{
    if (!rows_ || delta_index_ >= rows_->size()) {
        tuple = nullptr;
        return false;
    }
    tuple = &(*rows_)[delta_index_++];
    return true;
}

void WorkTableRegistry::RecursiveCursor::mark_delta_processed() noexcept
{
    if (!rows_) {
        delta_begin_ = 0U;
        delta_index_ = 0U;
        return;
    }
    delta_begin_ = rows_->size();
    delta_index_ = delta_begin_;
}

std::size_t WorkTableRegistry::RecursiveCursor::delta_count() const noexcept
{
    if (!rows_) {
        return 0U;
    }
    if (rows_->size() < delta_begin_) {
        return 0U;
    }
    return rows_->size() - delta_begin_;
}

std::shared_ptr<const std::vector<TupleBuffer>> WorkTableRegistry::find(std::uint64_t id,
                                                                        const txn::Snapshot& snapshot) const
{
    const auto it = tables_.find(id);
    if (it == tables_.end()) {
        return {};
    }
    if (!txn::snapshots_equal(it->second.snapshot, snapshot)) {
        return {};
    }
    return std::shared_ptr<const std::vector<TupleBuffer>>(it->second.rows);
}

std::shared_ptr<const std::vector<TupleBuffer>> WorkTableRegistry::publish(std::uint64_t id,
                                                                           txn::Snapshot snapshot,
                                                                           std::vector<TupleBuffer> rows)
{
    Entry entry{};
    entry.snapshot = std::move(snapshot);
    auto storage = std::make_shared<std::vector<TupleBuffer>>(std::move(rows));
    entry.rows = storage;
    auto shared_rows = std::shared_ptr<const std::vector<TupleBuffer>>(storage);
    tables_[id] = std::move(entry);
    return shared_rows;
}

std::optional<WorkTableRegistry::SnapshotIterator> WorkTableRegistry::snapshot_iterator(std::uint64_t id,
                                                                                       const txn::Snapshot& snapshot) const
{
    const auto it = tables_.find(id);
    if (it == tables_.end()) {
        return std::nullopt;
    }
    if (!txn::snapshots_equal(it->second.snapshot, snapshot)) {
        return std::nullopt;
    }
    if (!it->second.rows) {
        return std::nullopt;
    }
    auto shared_rows = std::shared_ptr<const std::vector<TupleBuffer>>(it->second.rows);
    return std::optional<SnapshotIterator>(SnapshotIterator{it->second.snapshot, std::move(shared_rows)});
}

std::optional<WorkTableRegistry::RecursiveCursor> WorkTableRegistry::recursive_cursor(std::uint64_t id,
                                                                                     const txn::Snapshot& snapshot) const
{
    const auto it = tables_.find(id);
    if (it == tables_.end()) {
        return std::nullopt;
    }
    if (!txn::snapshots_equal(it->second.snapshot, snapshot)) {
        return std::nullopt;
    }
    if (!it->second.rows) {
        return std::nullopt;
    }
    auto rows = it->second.rows;
    return std::optional<RecursiveCursor>(RecursiveCursor{it->second.snapshot, rows, rows->size()});
}

void WorkTableRegistry::clear() noexcept
{
    tables_.clear();
}

}  // namespace bored::executor
