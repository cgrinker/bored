#pragma once

#include "bored/ddl/ddl_errors.hpp"

#include <string_view>
#include <system_error>

namespace bored::ddl {

bool is_valid_identifier(std::string_view name) noexcept;
std::error_code validate_identifier(std::string_view name) noexcept;
std::error_code ensure_exists(bool exists, DdlErrc error_if_missing) noexcept;
std::error_code ensure_absent(bool present, DdlErrc error_if_present) noexcept;

}  // namespace bored::ddl
