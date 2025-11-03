#include "bored/executor/foreign_key_check_executor.hpp"

#include "bored/executor/executor_context.hpp"
#include "bored/executor/mvcc_visibility.hpp"

#include <span>
#include <stdexcept>
#include <utility>

namespace bored::executor {

namespace {

constexpr const char* kDefaultViolationMessage = "Foreign key constraint violated.";

}  // namespace

ForeignKeyCheckExecutor::ForeignKeyCheckExecutor(ExecutorNodePtr child, Config config)
    : config_{std::move(config)}
{
    if (!child) {
        throw std::invalid_argument{"ForeignKeyCheckExecutor requires a child executor"};
    }
    if (!config_.key_extractor) {
        throw std::invalid_argument{"ForeignKeyCheckExecutor requires a key extractor"};
    }
    add_child(std::move(child));
}

void ForeignKeyCheckExecutor::open(ExecutorContext& context)
{
    if (child_count() != 1U) {
        throw std::logic_error{"ForeignKeyCheckExecutor expected exactly one child"};
    }
    auto* input = child(0U);
    if (input == nullptr) {
        throw std::logic_error{"ForeignKeyCheckExecutor child is null"};
    }
    key_buffer_.clear();
    input->open(context);
    child_open_ = true;
}

bool ForeignKeyCheckExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    ExecutorTelemetry::LatencyScope latency_scope{config_.telemetry, ExecutorTelemetry::Operator::Filter};
    if (child_count() != 1U) {
        return false;
    }

    auto* input = child(0U);
    if (input == nullptr) {
        return false;
    }

    while (input->next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        key_buffer_.clear();
        bool has_null = false;
        if (!config_.key_extractor(view, context, key_buffer_, has_null)) {
            throw std::runtime_error{"Foreign key extraction failed"};
        }

        if (has_null && config_.skip_when_null) {
            return true;
        }

        if (config_.reader == nullptr || !config_.referenced_relation_id.is_valid() || !config_.referenced_index_id.is_valid()) {
            throw std::runtime_error{"ForeignKeyCheckExecutor missing storage configuration"};
        }

        storage::IndexProbeConfig probe{};
        probe.relation_id = config_.referenced_relation_id;
        probe.index_id = config_.referenced_index_id;
        probe.key = std::span<const std::byte>(key_buffer_.data(), key_buffer_.size());
        auto matches = config_.reader->probe_index(probe);

        bool satisfied = false;
        for (const auto& match : matches) {
            if (is_tuple_visible(match.tuple.header, context.transaction_id(), context.snapshot())) {
                satisfied = true;
                break;
            }
        }

        if (!satisfied) {
            throw std::runtime_error{violation_message()};
        }

        return true;
    }

    return false;
}

void ForeignKeyCheckExecutor::close(ExecutorContext& context)
{
    if (child_count() != 1U) {
        return;
    }
    auto* input = child(0U);
    if (input != nullptr && child_open_) {
        input->close(context);
    }
    child_open_ = false;
    key_buffer_.clear();
}

std::string ForeignKeyCheckExecutor::violation_message() const
{
    if (!config_.constraint_name.empty()) {
        return "Foreign key constraint '" + config_.constraint_name + "' violated.";
    }
    return kDefaultViolationMessage;
}

}  // namespace bored::executor
