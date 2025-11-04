#pragma once

#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace bored::executor {

class SpoolExecutor final : public ExecutorNode {
public:
    struct Config final {
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
        std::size_t reserve_rows = 0U;
    };

    SpoolExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

    void reset() noexcept;

private:
    void ensure_materialized(ExecutorContext& context);

    Config config_{};
    bool materialized_ = false;
    std::size_t position_ = 0U;
    std::vector<TupleBuffer> materialized_rows_{};
    TupleBuffer child_buffer_{};
    txn::Snapshot materialized_snapshot_{};
};

}  // namespace bored::executor
