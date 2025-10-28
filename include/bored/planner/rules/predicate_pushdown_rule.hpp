#pragma once

#include "bored/planner/rule.hpp"

namespace bored::planner {

std::shared_ptr<Rule> make_projection_pruning_rule();
std::shared_ptr<Rule> make_filter_pushdown_rule();
std::shared_ptr<Rule> make_constant_folding_rule();

}  // namespace bored::planner
