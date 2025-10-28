#pragma once

#include "bored/planner/logical_plan.hpp"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace bored::planner {

class PlannerContext;

enum class RuleCategory {
    Generic,
    Projection,
    Filter,
    ConstantFolding,
};

class RuleContext;

using RuleTransform = std::function<bool(const RuleContext&,
                                         const LogicalOperatorPtr&,
                                         std::vector<LogicalOperatorPtr>&)>;

class Rule final {
public:
    Rule(std::string name,
         std::vector<LogicalOperatorType> pattern,
         RuleCategory category = RuleCategory::Generic,
         RuleTransform transform = {},
         int priority = 0);

    [[nodiscard]] const std::string& name() const noexcept;
    [[nodiscard]] const std::vector<LogicalOperatorType>& pattern() const noexcept;
    [[nodiscard]] RuleCategory category() const noexcept;
    [[nodiscard]] int priority() const noexcept;

    bool apply(const RuleContext& context,
               const LogicalOperatorPtr& root,
               std::vector<LogicalOperatorPtr>& alternatives) const;

private:
    std::string name_{};
    std::vector<LogicalOperatorType> pattern_{};
    RuleCategory category_ = RuleCategory::Generic;
    int priority_ = 0;
    RuleTransform transform_{};
};

class RuleContext final {
public:
    explicit RuleContext(const PlannerContext* planner_context) noexcept;

    [[nodiscard]] const PlannerContext* planner_context() const noexcept;

private:
    const PlannerContext* planner_context_;
};

class RuleRegistry final {
public:
    void register_rule(std::shared_ptr<Rule> rule);

    [[nodiscard]] const std::vector<std::shared_ptr<Rule>>& rules() const noexcept;

private:
    std::vector<std::shared_ptr<Rule>> rules_{};
};

struct RuleApplication final {
    std::string rule_name;
    bool success = false;
};

struct RuleTrace final {
    std::vector<RuleApplication> applications{};
};

struct PlannerRuleOptions final {
    bool enable_projection_pushdown = true;
    bool enable_filter_pushdown = true;
    bool enable_constant_folding = false;
};

class RuleEngine final {
public:
    RuleEngine(const RuleRegistry* registry,
               PlannerRuleOptions options = {});

    bool apply_rules(const RuleContext& context,
                     const LogicalOperatorPtr& root,
                     std::vector<LogicalOperatorPtr>& results,
                     RuleTrace* trace = nullptr) const;

private:
    const RuleRegistry* registry_;
    PlannerRuleOptions options_{};

    [[nodiscard]] bool rule_enabled(const Rule& rule) const noexcept;
};

}  // namespace bored::planner
