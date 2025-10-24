#pragma once

#include <system_error>

namespace bored::ddl {

enum class DdlErrc {
    Success = 0,
    InvalidIdentifier,
    InvalidOption,
    SchemaNotFound,
    SchemaAlreadyExists,
    TableNotFound,
    TableAlreadyExists,
    IndexNotFound,
    IndexAlreadyExists,
    DatabaseNotFound,
    DatabaseAlreadyExists,
    HandlerMissing,
    ValidationFailed,
    ExecutionFailed
};

const std::error_category& ddl_error_category() noexcept;
std::error_code make_error_code(DdlErrc value) noexcept;

}  // namespace bored::ddl

namespace std {

template <>
struct is_error_code_enum<bored::ddl::DdlErrc> : true_type {
};

}  // namespace std
