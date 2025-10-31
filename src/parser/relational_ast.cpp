#include "bored/parser/relational/ast.hpp"

#include <iterator>
#include <sstream>
#include <string_view>

namespace bored::parser::relational {
namespace {

std::string describe_expression(const Expression& expression);

class ExpressionPrinter final : public ExpressionVisitor {
public:
    void visit(const IdentifierExpression& expression) override
    {
        result_ = format_qualified_name(expression.name);
    }

    void visit(const LiteralExpression& expression) override
    {
        switch (expression.tag) {
        case LiteralTag::Null:
            result_ = "NULL";
            break;
        case LiteralTag::Boolean:
            result_ = expression.boolean_value ? "TRUE" : "FALSE";
            break;
        case LiteralTag::Integer:
        case LiteralTag::Decimal:
            result_ = expression.text;
            break;
        case LiteralTag::String:
        default:
            result_.clear();
            result_.push_back('\'');
            result_.append(expression.text);
            result_.push_back('\'');
            break;
        }
    }

    void visit(const BinaryExpression& expression) override
    {
        static constexpr std::string_view operators[] = {
            " = ",
            " <> ",
            " < ",
            " <= ",
            " > ",
            " >= ",
            " + ",
            " - "
        };

        std::ostringstream stream;
        stream << '(';
        if (expression.left) {
            stream << describe_expression(*expression.left);
        } else {
            stream << "<null>";
        }

        const auto index = static_cast<std::size_t>(expression.op);
        if (index < std::size(operators)) {
            stream << operators[index];
        } else {
            stream << " ? ";
        }

        if (expression.right) {
            stream << describe_expression(*expression.right);
        } else {
            stream << "<null>";
        }
        stream << ')';
        result_ = stream.str();
    }

    void visit(const StarExpression& expression) override
    {
        if (expression.qualifier.empty()) {
            result_ = "*";
            return;
        }

        result_ = format_qualified_name(expression.qualifier);
        result_.append(".*");
    }

    [[nodiscard]] std::string take() && { return std::move(result_); }

private:
    std::string result_{};
};

std::string describe_expression(const Expression& expression)
{
    ExpressionPrinter printer{};
    expression.accept(printer);
    return std::move(printer).take();
}

std::string describe_order_by(const OrderByItem& item)
{
    std::ostringstream stream;
    if (item.expression) {
        stream << describe_expression(*item.expression);
    } else {
        stream << "<null>";
    }

    if (item.direction == OrderByItem::Direction::Descending) {
        stream << " DESC";
    }

    return stream.str();
}

}  // namespace

std::string format_qualified_name(const QualifiedName& name)
{
    std::ostringstream stream;
    bool first = true;
    for (const auto& part : name.parts) {
        if (!first) {
            stream << '.';
        }
        stream << part.value;
        first = false;
    }
    return stream.str();
}

std::string describe(const SelectStatement& statement)
{
    std::ostringstream stream;
    stream << "SELECT";

    if (!statement.query) {
        stream << " <invalid>";
        return stream.str();
    }

    const auto& query = *statement.query;
    if (query.distinct) {
        stream << " DISTINCT";
    }

    if (!query.select_items.empty()) {
        bool first_item = true;
        for (const auto* item : query.select_items) {
            if (!item) {
                continue;
            }
            stream << (first_item ? ' ' : ',');
            if (!first_item) {
                stream << ' ';
            }
            if (item->expression) {
                stream << describe_expression(*item->expression);
            } else {
                stream << "<null>";
            }
            if (item->alias) {
                stream << " AS " << item->alias->value;
            }
            first_item = false;
        }
    } else {
        stream << " <empty-select-list>";
    }

    if (!query.from_tables.empty()) {
        stream << " FROM ";
        bool first_table = true;
        for (const auto* table : query.from_tables) {
            if (!table) {
                continue;
            }
            if (!first_table) {
                stream << ", ";
            }
            stream << format_qualified_name(table->name);
            if (table->alias) {
                stream << " AS " << table->alias->value;
            }
            first_table = false;
        }
    }

    if (query.where) {
        stream << " WHERE " << describe_expression(*query.where);
    }

    if (!query.group_by.empty()) {
        stream << " GROUP BY ";
        bool first_group = true;
        for (const auto* expression : query.group_by) {
            if (!expression) {
                continue;
            }
            if (!first_group) {
                stream << ", ";
            }
            stream << describe_expression(*expression);
            first_group = false;
        }
    }

    if (!query.order_by.empty()) {
        stream << " ORDER BY ";
        bool first_order = true;
        for (const auto* item : query.order_by) {
            if (!item) {
                continue;
            }
            if (!first_order) {
                stream << ", ";
            }
            stream << describe_order_by(*item);
            first_order = false;
        }
    }

    if (query.limit && query.limit->row_count) {
        stream << " LIMIT " << describe_expression(*query.limit->row_count);
        if (query.limit->offset) {
            stream << " OFFSET " << describe_expression(*query.limit->offset);
        }
    }

    return stream.str();
}

}  // namespace bored::parser::relational
