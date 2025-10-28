#include "bored/executor/filter_executor.hpp"

#include <stdexcept>
#include <utility>

namespace bored::executor {

FilterExecutor::FilterExecutor(ExecutorNodePtr child, Config config)
    : config_{std::move(config)}
{
    if (!config_.predicate) {
        throw std::invalid_argument{"FilterExecutor requires a predicate"};
    }
    if (!child) {
        throw std::invalid_argument{"FilterExecutor requires a child executor"};
    }
    add_child(std::move(child));
}

void FilterExecutor::open(ExecutorContext& context)
{
    if (child_count() != 1U) {
        throw std::logic_error{"FilterExecutor expected exactly one child"};
    }
    auto* input = child(0U);
    if (input == nullptr) {
        throw std::logic_error{"FilterExecutor missing child executor"};
    }
    input->open(context);
}

bool FilterExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    if (child_count() != 1U) {
        return false;
    }

    auto* input = child(0U);
    if (input == nullptr) {
        return false;
    }

    while (input->next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        const bool passed = config_.predicate(view, context);
        if (config_.telemetry != nullptr) {
            config_.telemetry->record_filter_row(passed);
        }
        if (passed) {
            return true;
        }
    }

    return false;
}

void FilterExecutor::close(ExecutorContext& context)
{
    if (child_count() != 1U) {
        return;
    }
    auto* input = child(0U);
    if (input != nullptr) {
        input->close(context);
    }
}

}  // namespace bored::executor
