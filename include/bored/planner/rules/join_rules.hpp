#pragma once

#include "bored/planner/rule.hpp"

namespace bored::planner {

std::shared_ptr<Rule> make_join_commutativity_rule();
std::shared_ptr<Rule> make_join_associativity_rule();
std::shared_ptr<Rule> make_join_greedy_reorder_rule();

}  // namespace bored::planner
