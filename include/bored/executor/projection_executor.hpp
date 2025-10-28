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

class ProjectionExecutor final : public ExecutorNode {
public:
    using Projection = std::function<void(const TupleView&, TupleWriter&, ExecutorContext&)>;

    struct Config final {
        std::vector<Projection> projections;
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    ProjectionExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    Config config_{};
    TupleBuffer child_buffer_{};
};

}  // namespace bored::executor
