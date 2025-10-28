#include "bored/planner/memo.hpp"

#include <utility>

namespace bored::planner {

void MemoGroup::add_expression(LogicalOperatorPtr expression)
{
    if (expression) {
        expressions_.push_back(std::move(expression));
    }
}

const std::vector<LogicalOperatorPtr>& MemoGroup::expressions() const noexcept
{
    return expressions_;
}

Memo::GroupId Memo::add_group(LogicalOperatorPtr expression)
{
    groups_.emplace_back();
    auto id = static_cast<GroupId>(groups_.size() - 1U);
    if (expression) {
        groups_.back().add_expression(std::move(expression));
    }
    return id;
}

void Memo::add_expression(GroupId group, LogicalOperatorPtr expression)
{
    if (!expression || group >= groups_.size()) {
        return;
    }
    groups_[group].add_expression(std::move(expression));
}

const MemoGroup* Memo::find_group(GroupId group) const noexcept
{
    if (group >= groups_.size()) {
        return nullptr;
    }
    return &groups_[group];
}

MemoGroup* Memo::find_group(GroupId group) noexcept
{
    if (group >= groups_.size()) {
        return nullptr;
    }
    return &groups_[group];
}

const std::vector<MemoGroup>& Memo::groups() const noexcept
{
    return groups_;
}

}  // namespace bored::planner
