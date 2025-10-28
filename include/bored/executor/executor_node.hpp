#pragma once

#include <cstddef>
#include <memory>
#include <vector>

namespace bored::executor {

class ExecutorContext;
class TupleBuffer;

class ExecutorNode;
using ExecutorNodePtr = std::unique_ptr<ExecutorNode>;

class ExecutorNode {
public:
    virtual ~ExecutorNode() = default;

    ExecutorNode(const ExecutorNode&) = delete;
    ExecutorNode& operator=(const ExecutorNode&) = delete;
    ExecutorNode(ExecutorNode&&) = default;
    ExecutorNode& operator=(ExecutorNode&&) = default;

    virtual void open(ExecutorContext& context) = 0;
    virtual bool next(ExecutorContext& context, TupleBuffer& buffer) = 0;
    virtual void close(ExecutorContext& context) = 0;

    void add_child(ExecutorNodePtr child);

    [[nodiscard]] std::size_t child_count() const noexcept;
    [[nodiscard]] ExecutorNode* child(std::size_t index) const noexcept;
    [[nodiscard]] const std::vector<ExecutorNodePtr>& children() const noexcept;

protected:
    ExecutorNode() = default;

private:
    std::vector<ExecutorNodePtr> children_{};
};

}  // namespace bored::executor
