#include "bored/executor/worktable_registry.hpp"

#include <utility>

namespace bored::executor {

WorkTableRegistry::SnapshotIterator::SnapshotIterator(txn::Snapshot snapshot,
                                                      std::shared_ptr<const std::vector<TupleBuffer>> rows) noexcept
    : snapshot_{snapshot}
    , rows_{std::move(rows)}
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
    return it->second.rows;
}

std::shared_ptr<const std::vector<TupleBuffer>> WorkTableRegistry::publish(std::uint64_t id,
                                                                           txn::Snapshot snapshot,
                                                                           std::vector<TupleBuffer> rows)
{
    Entry entry{};
    entry.snapshot = std::move(snapshot);
    entry.rows = std::make_shared<std::vector<TupleBuffer>>(std::move(rows));
    auto shared_rows = entry.rows;
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
    return std::optional<SnapshotIterator>(SnapshotIterator{it->second.snapshot, it->second.rows});
}

void WorkTableRegistry::clear() noexcept
{
    tables_.clear();
}

}  // namespace bored::executor
