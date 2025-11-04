#pragma once

#include "bored/planner/logical_plan.hpp"

#include <cstddef>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace bored::planner {

class MemoGroup final {
public:
    void add_expression(LogicalOperatorPtr expression);

    [[nodiscard]] const std::vector<LogicalOperatorPtr>& expressions() const noexcept;

private:
    std::vector<LogicalOperatorPtr> expressions_{};
};

class Memo final {
public:
    using GroupId = std::size_t;

    static constexpr GroupId invalid_group() noexcept
    {
        return std::numeric_limits<GroupId>::max();
    }

    [[nodiscard]] GroupId add_group(LogicalOperatorPtr expression = nullptr);
    void add_expression(GroupId group, LogicalOperatorPtr expression);

    [[nodiscard]] const MemoGroup* find_group(GroupId group) const noexcept;
    [[nodiscard]] MemoGroup* find_group(GroupId group) noexcept;

    [[nodiscard]] const std::vector<MemoGroup>& groups() const noexcept;

private:
    std::vector<MemoGroup> groups_{};
    std::unordered_map<std::size_t, std::vector<GroupId>> expression_index_{};
    std::unordered_set<GroupId> materialized_groups_{};

    void ensure_materialized_alternative(GroupId group, const LogicalOperatorPtr& representative);
};

}  // namespace bored::planner
