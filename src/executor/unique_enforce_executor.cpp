#include "bored/executor/unique_enforce_executor.hpp"

#include "bored/executor/executor_context.hpp"
#include "bored/executor/mvcc_visibility.hpp"

#include <span>
#include <stdexcept>
#include <utility>

namespace bored::executor {

namespace {

constexpr const char* kDefaultViolationMessage = "Unique constraint violated.";

}  // namespace

UniqueEnforceExecutor::UniqueEnforceExecutor(ExecutorNodePtr child, Config config)
    : config_{std::move(config)}
{
    if (!child) {
        throw std::invalid_argument{"UniqueEnforceExecutor requires a child executor"};
    }
    if (!config_.key_extractor) {
        throw std::invalid_argument{"UniqueEnforceExecutor requires a key extractor"};
    }
    add_child(std::move(child));
}

void UniqueEnforceExecutor::open(ExecutorContext& context)
{
    if (child_count() != 1U) {
        throw std::logic_error{"UniqueEnforceExecutor expected exactly one child"};
    }
    auto* input = child(0U);
    if (input == nullptr) {
        throw std::logic_error{"UniqueEnforceExecutor child is null"};
    }
    seen_keys_.clear();
    key_buffer_.clear();
    input->open(context);
    child_open_ = true;
}

bool UniqueEnforceExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
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
            throw std::runtime_error{"Unique constraint key extraction failed"};
        }

        const bool key_present = !key_buffer_.empty();
        if (has_null) {
            if (!config_.allow_null_keys && key_present) {
                throw std::runtime_error{violation_message()};
            }
            if (config_.allow_null_keys) {
                return true;
            }
        }

        if (key_present) {
            auto [_, inserted] = seen_keys_.insert(key_buffer_);
            if (!inserted) {
                throw std::runtime_error{violation_message()};
            }

            if (config_.reader != nullptr && config_.index_id.is_valid()) {
                storage::IndexProbeConfig probe{};
                probe.relation_id = config_.relation_id;
                probe.index_id = config_.index_id;
                probe.key = std::span<const std::byte>(key_buffer_.data(), key_buffer_.size());
                auto matches = config_.reader->probe_index(probe);
                for (const auto& match : matches) {
                    if (config_.ignore_match && config_.ignore_match(view, match.tuple, context)) {
                        continue;
                    }
                    const bool visible = is_tuple_visible(match.tuple.header, context.transaction_id(), context.snapshot());
                    if (visible) {
                        throw std::runtime_error{violation_message()};
                    }
                }
            }
        }

        return true;
    }

    return false;
}

void UniqueEnforceExecutor::close(ExecutorContext& context)
{
    if (child_count() != 1U) {
        return;
    }
    auto* input = child(0U);
    if (input != nullptr && child_open_) {
        input->close(context);
    }
    child_open_ = false;
    seen_keys_.clear();
    key_buffer_.clear();
}

std::string UniqueEnforceExecutor::violation_message() const
{
    if (!config_.constraint_name.empty()) {
        return "Unique constraint '" + config_.constraint_name + "' violated.";
    }
    return kDefaultViolationMessage;
}

}  // namespace bored::executor
