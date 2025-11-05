#include "bored/planner/memo.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <string>
#include <utility>

namespace bored::planner {

namespace {

std::size_t hash_combine(std::size_t seed, std::size_t value) noexcept
{
    constexpr std::size_t kMagic = 0x9e3779b97f4a7c15ULL;
    seed ^= value + kMagic + (seed << 6U) + (seed >> 2U);
    return seed;
}

std::size_t expression_fingerprint(const LogicalOperatorPtr& expression)
{
    if (!expression) {
        return 0U;
    }

    std::size_t seed = static_cast<std::size_t>(expression->type());

    const auto& props = expression->properties();
    seed = hash_combine(seed, props.estimated_cardinality);
    seed = hash_combine(seed, props.preserves_order ? 1U : 0U);
    seed = hash_combine(seed, static_cast<std::size_t>(props.relation_id.value));

    std::hash<std::string> string_hash;
    seed = hash_combine(seed, string_hash(props.relation_name));
    seed = hash_combine(seed, props.output_columns.size());
    for (const auto& column : props.output_columns) {
        seed = hash_combine(seed, string_hash(column));
    }

    const auto& children = expression->children();
    seed = hash_combine(seed, children.size());
    for (const auto& child : children) {
        seed = hash_combine(seed, expression_fingerprint(child));
    }

    return seed;
}

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
    if (expression) {
        const auto fingerprint = expression_fingerprint(expression);
        if (auto it = expression_index_.find(fingerprint); it != expression_index_.end()) {
            for (const auto candidate : it->second) {
                const auto* group = find_group(candidate);
                if (group == nullptr) {
                    continue;
                }
                for (const auto& existing : group->expressions()) {
                    if (expressions_equivalent(existing, expression)) {
                        if (existing.get() != expression.get()) {
                            ensure_materialized_alternative(candidate, existing);
                        }
                        return candidate;
                    }
                }
            }
        }

        groups_.emplace_back();
        const auto id = static_cast<GroupId>(groups_.size() - 1U);
        groups_.back().add_expression(expression);
        auto& bucket = expression_index_[fingerprint];
        if (std::find(bucket.begin(), bucket.end(), id) == bucket.end()) {
            bucket.push_back(id);
        }
        return id;
    }

    groups_.emplace_back();
    return static_cast<GroupId>(groups_.size() - 1U);
}

void Memo::add_expression(GroupId group, LogicalOperatorPtr expression)
{
    if (!expression || group >= groups_.size()) {
        return;
    }
    auto& memo_group = groups_[group];
    for (const auto& existing : memo_group.expressions()) {
        if (expressions_equivalent(existing, expression)) {
            return;
        }
    }

    memo_group.add_expression(expression);
    const auto fingerprint = expression_fingerprint(expression);
    auto& bucket = expression_index_[fingerprint];
    if (std::find(bucket.begin(), bucket.end(), group) == bucket.end()) {
        bucket.push_back(group);
    }
}

void Memo::ensure_materialized_alternative(GroupId group, const LogicalOperatorPtr& representative)
{
    if (group >= groups_.size() || !representative) {
        return;
    }
    if (!materialized_groups_.insert(group).second) {
        return;
    }

    LogicalProperties materialize_props = representative->properties();
    materialize_props.requires_recursive_cursor = representative->properties().requires_recursive_cursor;
    auto materialize = LogicalOperator::make(
        LogicalOperatorType::Materialize,
        std::vector<LogicalOperatorPtr>{representative},
        std::move(materialize_props));

    groups_[group].add_expression(std::move(materialize));
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
