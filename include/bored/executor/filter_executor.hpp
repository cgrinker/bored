#pragma once

#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_format.hpp"

#include <functional>
#include <memory>
#include <string>

namespace bored::executor {

class FilterExecutor final : public ExecutorNode {
public:
    using Predicate = std::function<bool(const TupleView&, ExecutorContext&)>;

    struct Config final {
        Predicate predicate;
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    FilterExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    Config config_{};
};

}  // namespace bored::executor
