#pragma once

#include "bored/shell/shell_engine.hpp"

#include <string>

namespace bored::tools {

[[nodiscard]] std::string format_shell_command_log_json(const bored::shell::CommandMetrics& metrics);

}  // namespace bored::tools
