#pragma once

#include "bored/parser/relational/logical_plan.hpp"

#include <string>

namespace bored::parser::relational {

[[nodiscard]] std::string describe_plan(const LogicalOperator& root);

}  // namespace bored::parser::relational
