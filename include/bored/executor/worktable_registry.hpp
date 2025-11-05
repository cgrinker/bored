#pragma once

#include "bored/executor/tuple_buffer.hpp"
#include "bored/txn/snapshot_utils.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

namespace bored::executor {

class WorkTableRegistry final {
public:
    WorkTableRegistry() = default;

    class SnapshotIterator final {
    public:
        SnapshotIterator() = default;

        SnapshotIterator(txn::Snapshot snapshot, std::shared_ptr<const std::vector<TupleBuffer>> rows) noexcept;

        [[nodiscard]] bool valid() const noexcept { return static_cast<bool>(rows_); }
        [[nodiscard]] const txn::Snapshot& snapshot() const noexcept { return snapshot_; }
        [[nodiscard]] bool matches(const txn::Snapshot& snapshot) const noexcept;
        [[nodiscard]] std::size_t size() const noexcept;
        [[nodiscard]] std::size_t position() const noexcept { return index_; }
        void reset() noexcept { index_ = 0U; }
        [[nodiscard]] bool next(const TupleBuffer*& tuple) noexcept;
        [[nodiscard]] std::span<const TupleBuffer> span() const noexcept;

    private:
        txn::Snapshot snapshot_{};
        std::shared_ptr<const std::vector<TupleBuffer>> rows_{};
        std::size_t index_ = 0U;
    };

    [[nodiscard]] std::shared_ptr<const std::vector<TupleBuffer>> find(std::uint64_t id, const txn::Snapshot& snapshot) const;
    [[nodiscard]] std::shared_ptr<const std::vector<TupleBuffer>> publish(std::uint64_t id,
                                                                         txn::Snapshot snapshot,
                                                                         std::vector<TupleBuffer> rows);
    [[nodiscard]] std::optional<SnapshotIterator> snapshot_iterator(std::uint64_t id, const txn::Snapshot& snapshot) const;
    void clear() noexcept;

private:
    struct Entry final {
        txn::Snapshot snapshot{};
        std::shared_ptr<const std::vector<TupleBuffer>> rows{};
    };

    std::unordered_map<std::uint64_t, Entry> tables_{};
};

}  // namespace bored::executor
