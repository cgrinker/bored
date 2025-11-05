#pragma once

#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/worktable_registry.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace bored::executor {

class SpoolExecutor final : public ExecutorNode {
public:
    struct Config final {
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
        std::size_t reserve_rows = 0U;
        WorkTableRegistry* worktable_registry = nullptr;
        std::optional<std::uint64_t> worktable_id{};
    };

    SpoolExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

    void reset() noexcept;

    [[nodiscard]] std::optional<WorkTableRegistry::SnapshotIterator> snapshot_iterator() const;
    [[nodiscard]] std::optional<WorkTableRegistry::RecursiveCursor> recursive_cursor() const;

private:
    void ensure_materialized(ExecutorContext& context);

    Config config_{};
    bool materialized_ = false;
    std::size_t position_ = 0U;
    std::vector<TupleBuffer> materialized_rows_{};
    TupleBuffer child_buffer_{};
    txn::Snapshot materialized_snapshot_{};
    std::shared_ptr<const std::vector<TupleBuffer>> shared_rows_{};
};

}  // namespace bored::executor
