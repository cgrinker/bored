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

namespace bored::txn {
class TransactionContext;
}

namespace bored::executor {

class UpdateExecutor final : public ExecutorNode {
public:
    struct UpdateStats final {
        std::size_t new_payload_bytes = 0U;
        std::size_t old_payload_bytes = 0U;
        std::size_t wal_bytes = 0U;
    };

    class Target {
    public:
        virtual ~Target() = default;

        virtual std::error_code update_tuple(const TupleView& tuple,
                                             ExecutorContext& context,
                                             UpdateStats& out_stats) = 0;
        virtual std::error_code flush(ExecutorContext& context)
        {
            (void)context;
            return {};
        }
        virtual std::error_code register_transaction_hooks(txn::TransactionContext& txn,
                                                            ExecutorContext& context)
        {
            (void)txn;
            (void)context;
            return {};
        }
    };

    struct Config final {
        Target* target = nullptr;
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    UpdateExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    void ensure_child_available() const;
    void drain_child(ExecutorContext& context);
    void apply_telemetry_attempt() const;
    void apply_telemetry_success(const UpdateStats& stats) const;
    void finalize_target(ExecutorContext& context);

    Config config_{};
    TupleBuffer child_buffer_{};
    bool child_open_ = false;
    bool drained_ = false;
    bool transaction_hooks_registered_ = false;
};

}  // namespace bored::executor
