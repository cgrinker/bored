#include "bored/executor/executor_node.hpp"

#include <stdexcept>

namespace bored::executor {

void ExecutorNode::add_child(ExecutorNodePtr child)
{
    if (!child) {
        throw std::invalid_argument("executor child must not be null");
    }
    children_.push_back(std::move(child));
}

std::size_t ExecutorNode::child_count() const noexcept
{
    return children_.size();
}

ExecutorNode* ExecutorNode::child(std::size_t index) const noexcept
{
    if (index >= children_.size()) {
        return nullptr;
    }
    return children_[index].get();
}

const std::vector<ExecutorNodePtr>& ExecutorNode::children() const noexcept
{
    return children_;
}

}  // namespace bored::executor
