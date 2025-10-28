#pragma once

#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/tuple_format.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>

namespace bored::executor {

class InsertExecutor final : public ExecutorNode {
public:
    struct InsertStats final {
        std::size_t payload_bytes = 0U;
        std::size_t wal_bytes = 0U;
    };

    class Target {
    public:
        virtual ~Target() = default;

        virtual std::error_code insert_tuple(const TupleView& tuple,
                                             ExecutorContext& context,
                                             InsertStats& out_stats) = 0;
        virtual std::error_code flush(ExecutorContext& context)
        {
            (void)context;
            return {};
        }
    };

    struct Config final {
        Target* target = nullptr;
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    InsertExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    void ensure_child_available() const;
    void drain_child(ExecutorContext& context);
    void apply_telemetry_attempt() const;
    void apply_telemetry_success(const InsertStats& stats) const;

    Config config_{};
    TupleBuffer child_buffer_{};
    bool child_open_ = false;
    bool drained_ = false;
};

}  // namespace bored::executor
