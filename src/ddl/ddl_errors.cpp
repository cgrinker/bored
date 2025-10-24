#include "bored/ddl/ddl_errors.hpp"

#include <array>

namespace bored::ddl {

namespace {

class DdlErrorCategory final : public std::error_category {
public:
    const char* name() const noexcept override
    {
        return "bored.ddl";
    }

    std::string message(int condition) const override
    {
        switch (static_cast<DdlErrc>(condition)) {
        case DdlErrc::Success:
            return "success";
        case DdlErrc::InvalidIdentifier:
            return "invalid identifier";
        case DdlErrc::InvalidOption:
            return "invalid option";
        case DdlErrc::SchemaNotFound:
            return "schema not found";
        case DdlErrc::SchemaAlreadyExists:
            return "schema already exists";
        case DdlErrc::TableNotFound:
            return "table not found";
        case DdlErrc::TableAlreadyExists:
            return "table already exists";
        case DdlErrc::IndexNotFound:
            return "index not found";
        case DdlErrc::IndexAlreadyExists:
            return "index already exists";
        case DdlErrc::DatabaseNotFound:
            return "database not found";
        case DdlErrc::DatabaseAlreadyExists:
            return "database already exists";
        case DdlErrc::HandlerMissing:
            return "ddl handler missing";
        case DdlErrc::ValidationFailed:
            return "ddl validation failed";
        case DdlErrc::ExecutionFailed:
            return "ddl execution failed";
        default:
            return "unknown ddl error";
        }
    }
};

const DdlErrorCategory kCategory{};

}  // namespace

const std::error_category& ddl_error_category() noexcept
{
    return kCategory;
}

std::error_code make_error_code(DdlErrc value) noexcept
{
    return {static_cast<int>(value), ddl_error_category()};
}

}  // namespace bored::ddl
