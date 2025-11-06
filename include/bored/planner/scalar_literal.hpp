#pragma once

#include <cstdint>
#include <string>
#include <variant>

namespace bored::planner {

using ScalarLiteralValue = std::variant<std::int64_t, std::string>;

}  // namespace bored::planner
