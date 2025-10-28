#pragma once

#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/tuple_format.hpp"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace bored::executor {

class NestedLoopJoinExecutor final : public ExecutorNode {
public:
    using JoinPredicate = std::function<bool(const TupleView&, const TupleView&, ExecutorContext&)>;
    using JoinProjection = std::function<void(const TupleView&, const TupleView&, TupleWriter&, ExecutorContext&)>;
    using ProbeRebinder = std::function<void(const TupleView&, ExecutorContext&)>;

    struct Config final {
        JoinPredicate predicate{};
        std::vector<JoinProjection> projections{};
        ProbeRebinder rebind_probe{};
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    NestedLoopJoinExecutor(ExecutorNodePtr outer, ExecutorNodePtr inner, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    [[nodiscard]] ExecutorNode* outer_child() const noexcept;
    [[nodiscard]] ExecutorNode* inner_child() const noexcept;
    void prepare_inner(const TupleView& outer_view, ExecutorContext& context);
    void append_view_columns(const TupleView& view, TupleWriter& writer) const;

    Config config_{};
    TupleBuffer outer_buffer_{};
    TupleBuffer inner_buffer_{};
    TupleView outer_view_{};
    bool outer_view_valid_ = false;
    bool inner_open_ = false;
};

}  // namespace bored::executor
