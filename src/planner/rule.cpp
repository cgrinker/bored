#include "bored/planner/rule.hpp"

#include <algorithm>
#include <utility>

namespace bored::planner {

namespace {

bool matches_pattern(const std::vector<LogicalOperatorType>& pattern,
                     const LogicalOperatorPtr& root) noexcept
{
    if (!root) {
        return false;
    }
    if (pattern.empty()) {
        return false;
    }
    if (root->type() != pattern.front()) {
        return false;
    }
    if (pattern.size() == 1) {
        return true;
    }
    if (root->children().empty()) {
        return false;
    }
    return matches_pattern({pattern.begin() + 1, pattern.end()}, root->children().front());
}

}  // namespace

Rule::Rule(std::string name,
           std::vector<LogicalOperatorType> pattern,
           RuleCategory category,
           RuleTransform transform,
           int priority)
    : name_{std::move(name)}
    , pattern_{std::move(pattern)}
    , category_{category}
    , priority_{priority}
    , transform_{std::move(transform)}
{
}

const std::string& Rule::name() const noexcept
{
    return name_;
}

const std::vector<LogicalOperatorType>& Rule::pattern() const noexcept
{
    return pattern_;
}

RuleCategory Rule::category() const noexcept
{
    return category_;
}

int Rule::priority() const noexcept
{
    return priority_;
}

bool Rule::apply(const RuleContext& context,
                 const LogicalOperatorPtr& root,
                 std::vector<LogicalOperatorPtr>& alternatives) const
{
    if (!matches_pattern(pattern_, root)) {
        return false;
    }

    if (!transform_) {
        return false;
    }

    const auto applied = transform_(context, root, alternatives);
    if (applied) {
        if (auto* memo = context.memo(); memo && context.target_group() != Memo::invalid_group()) {
            for (const auto& alternative : alternatives) {
                memo->add_expression(context.target_group(), alternative);
            }
        }
    }
    return applied;
}

RuleContext::RuleContext(const PlannerContext* planner_context,
                         Memo* memo,
                         Memo::GroupId target_group) noexcept
    : planner_context_{planner_context}
    , memo_{memo}
    , target_group_{target_group}
{
}

const PlannerContext* RuleContext::planner_context() const noexcept
{
    return planner_context_;
}

Memo* RuleContext::memo() const noexcept
{
    return memo_;
}

Memo::GroupId RuleContext::target_group() const noexcept
{
    return target_group_;
}

void RuleRegistry::register_rule(std::shared_ptr<Rule> rule)
{
    rules_.push_back(std::move(rule));
    std::stable_sort(rules_.begin(), rules_.end(), [](const auto& lhs, const auto& rhs) {
        return lhs->priority() > rhs->priority();
    });
}

const std::vector<std::shared_ptr<Rule>>& RuleRegistry::rules() const noexcept
{
    return rules_;
}

RuleEngine::RuleEngine(const RuleRegistry* registry, PlannerRuleOptions options)
    : registry_{registry}
    , options_{std::move(options)}
{
}

bool RuleEngine::apply_rules(const RuleContext& context,
                             const LogicalOperatorPtr& root,
                             std::vector<LogicalOperatorPtr>& results,
                             RuleTrace* trace) const
{
    if (registry_ == nullptr || !root) {
        return false;
    }

    bool applied_any = false;

    for (const auto& rule : registry_->rules()) {
        if (trace) {
            trace->applications.push_back({rule->name(), false});
        }

        if (!rule_enabled(*rule)) {
            continue;
        }

        std::vector<LogicalOperatorPtr> alternatives;
        if (rule->apply(context, root, alternatives)) {
            applied_any = true;
            results.insert(results.end(), alternatives.begin(), alternatives.end());
            if (trace) {
                trace->applications.back().success = true;
            }
        }
    }

    return applied_any;
}

bool RuleEngine::rule_enabled(const Rule& rule) const noexcept
{
    switch (rule.category()) {
    case RuleCategory::Projection:
        return options_.enable_projection_pushdown;
    case RuleCategory::Filter:
        return options_.enable_filter_pushdown;
    case RuleCategory::ConstantFolding:
        return options_.enable_constant_folding;
    case RuleCategory::Generic:
    default:
        return true;
    }
}

}  // namespace bored::planner
