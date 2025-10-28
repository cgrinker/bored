#include "bored/planner/memo.hpp"

#include <cstddef>
#include <utility>

namespace bored::planner {

namespace {

bool expressions_equivalent(const LogicalOperatorPtr& lhs, const LogicalOperatorPtr& rhs)
{
    if (lhs == rhs) {
        return true;
    }
    if (!lhs || !rhs) {
        return false;
    }
    if (lhs->type() != rhs->type()) {
        return false;
    }

    const auto& lhs_props = lhs->properties();
    const auto& rhs_props = rhs->properties();
    if (lhs_props.estimated_cardinality != rhs_props.estimated_cardinality ||
        lhs_props.preserves_order != rhs_props.preserves_order ||
        lhs_props.relation_name != rhs_props.relation_name ||
        lhs_props.output_columns != rhs_props.output_columns) {
        return false;
    }

    const auto& lhs_children = lhs->children();
    const auto& rhs_children = rhs->children();
    if (lhs_children.size() != rhs_children.size()) {
        return false;
    }

    for (std::size_t i = 0; i < lhs_children.size(); ++i) {
        if (!expressions_equivalent(lhs_children[i], rhs_children[i])) {
            return false;
        }
    }

    return true;
}

}  // namespace

void MemoGroup::add_expression(LogicalOperatorPtr expression)
{
    if (!expression) {
        return;
    }
    for (const auto& existing : expressions_) {
        if (expressions_equivalent(existing, expression)) {
            return;
        }
    }
    expressions_.push_back(std::move(expression));
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
