#include "bored/ddl/ddl_validation.hpp"

#include <cctype>

namespace bored::ddl {

namespace {

bool is_valid_identifier_char(unsigned char ch) noexcept
{
    return std::isalnum(ch) != 0 || ch == '_' || ch == '$';
}

bool is_valid_identifier_start(unsigned char ch) noexcept
{
    return std::isalpha(ch) != 0 || ch == '_' || ch == '$';
}

}  // namespace

bool is_valid_identifier(std::string_view name) noexcept
{
    if (name.empty()) {
        return false;
    }
    if (!is_valid_identifier_start(static_cast<unsigned char>(name.front()))) {
        return false;
    }
    for (char ch : name) {
        if (!is_valid_identifier_char(static_cast<unsigned char>(ch))) {
            return false;
        }
    }
    return true;
}

std::error_code validate_identifier(std::string_view name) noexcept
{
    if (is_valid_identifier(name)) {
        return {};
    }
    return make_error_code(DdlErrc::InvalidIdentifier);
}

std::error_code ensure_exists(bool exists, DdlErrc error_if_missing) noexcept
{
    if (exists) {
        return {};
    }
    return make_error_code(error_if_missing);
}

std::error_code ensure_absent(bool present, DdlErrc error_if_present) noexcept
{
    if (!present) {
        return {};
    }
    return make_error_code(error_if_present);
}

}  // namespace bored::ddl
