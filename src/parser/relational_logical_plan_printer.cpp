#include "bored/parser/relational/logical_plan_printer.hpp"

#include "bored/parser/relational/ast.hpp"

#include <sstream>
#include <string>

namespace bored::parser::relational {
namespace {

std::string scalar_type_name(ScalarType type)
{
    switch (type) {
    case ScalarType::Boolean:
        return "BOOLEAN";
    case ScalarType::Int64:
        return "INT64";
    case ScalarType::UInt32:
        return "UINT32";
    case ScalarType::Decimal:
        return "DECIMAL";
    case ScalarType::Utf8:
        return "UTF8";
    default:
        return "UNKNOWN";
    }
}

std::string join_type_name(JoinType type)
{
    switch (type) {
    case JoinType::Inner:
        return "Inner";
    case JoinType::LeftOuter:
        return "LeftOuter";
    case JoinType::RightOuter:
        return "RightOuter";
    case JoinType::FullOuter:
        return "FullOuter";
    case JoinType::Cross:
        return "Cross";
    default:
        return "Unknown";
    }
}

std::string indent(std::size_t depth)
{
    return std::string(depth * 2U, ' ');
}

std::string describe_expression(const Expression& expression);

std::string describe_identifier(const IdentifierExpression& expression)
{
    if (expression.name.parts.empty()) {
        if (expression.binding.has_value()) {
            const auto& binding = *expression.binding;
            std::ostringstream stream;
            if (binding.table_alias.has_value() && !binding.table_alias->empty()) {
                stream << *binding.table_alias;
            } else if (!binding.table_name.empty()) {
                stream << binding.table_name;
            }
            if (!binding.column_name.empty()) {
                if (!stream.str().empty()) {
                    stream << '.';
                }
                stream << binding.column_name;
            }
            auto text = stream.str();
            if (!text.empty()) {
                return text;
            }
        }
        return "<identifier>";
    }
    return format_qualified_name(expression.name);
}

std::string describe_literal(const LiteralExpression& expression)
{
    switch (expression.tag) {
    case LiteralTag::Boolean:
        return expression.boolean_value ? "TRUE" : "FALSE";
    case LiteralTag::Integer:
    case LiteralTag::Decimal:
        return expression.text;
    case LiteralTag::String: {
        std::ostringstream stream;
        stream << '\'' << expression.text << '\'';
        return stream.str();
    }
    case LiteralTag::Null:
        return "NULL";
    default:
        return "<literal>";
    }
}

std::string describe_expression(const Expression& expression)
{
    switch (expression.kind) {
    case NodeKind::IdentifierExpression:
        return describe_identifier(static_cast<const IdentifierExpression&>(expression));
    case NodeKind::LiteralExpression:
        return describe_literal(static_cast<const LiteralExpression&>(expression));
    case NodeKind::BinaryExpression: {
        const auto& binary = static_cast<const BinaryExpression&>(expression);
        std::ostringstream stream;
        stream << '(';
        if (binary.left != nullptr) {
            stream << describe_expression(*binary.left);
        } else {
            stream << "<null>";
        }
        stream << ' ';
        switch (binary.op) {
        case BinaryOperator::Equal:
            stream << '=';
            break;
        case BinaryOperator::NotEqual:
            stream << "<>";
            break;
        case BinaryOperator::Less:
            stream << '<';
            break;
        case BinaryOperator::LessOrEqual:
            stream << "<=";
            break;
        case BinaryOperator::Greater:
            stream << '>';
            break;
        case BinaryOperator::GreaterOrEqual:
            stream << ">=";
            break;
        default:
            stream << '?';
            break;
        }
        stream << ' ';
        if (binary.right != nullptr) {
            stream << describe_expression(*binary.right);
        } else {
            stream << "<null>";
        }
        stream << ')';
        return stream.str();
    }
    case NodeKind::StarExpression:
        return "*";
    default:
        return "<expression>";
    }
}

void describe_plan(const LogicalOperator& node, std::size_t depth, std::ostringstream& stream)
{
    stream << indent(depth);
    switch (node.kind) {
    case LogicalOperatorKind::Scan: {
        const auto& scan = static_cast<const LogicalScan&>(node);
        stream << "Scan table=" << scan.table.schema_name << '.' << scan.table.table_name;
        if (scan.table.table_alias.has_value()) {
            stream << " alias=" << *scan.table.table_alias;
        }
        stream << '\n';
        break;
    }
    case LogicalOperatorKind::Filter: {
        const auto& filter = static_cast<const LogicalFilter&>(node);
        stream << "Filter";
        if (filter.predicate != nullptr) {
            stream << " predicate=" << describe_expression(*filter.predicate);
        }
        stream << '\n';
        if (filter.input != nullptr) {
            describe_plan(*filter.input, depth + 1U, stream);
        }
        break;
    }
    case LogicalOperatorKind::Join: {
        const auto& join = static_cast<const LogicalJoin&>(node);
        stream << "Join type=" << join_type_name(join.join_type);
        if (join.predicate != nullptr) {
            stream << " predicate=" << describe_expression(*join.predicate);
        }
        stream << '\n';
        if (join.left != nullptr) {
            describe_plan(*join.left, depth + 1U, stream);
        }
        if (join.right != nullptr) {
            describe_plan(*join.right, depth + 1U, stream);
        }
        break;
    }
    case LogicalOperatorKind::Project: {
        const auto& project = static_cast<const LogicalProject&>(node);
        stream << "Project columns=[";
        bool first = true;
        for (const auto& column : project.output_schema) {
            if (!first) {
                stream << ", ";
            }
            stream << column.name << ':' << scalar_type_name(column.type);
            stream << (column.nullable ? "?" : "");
            first = false;
        }
        stream << "]\n";
        if (project.input != nullptr) {
            describe_plan(*project.input, depth + 1U, stream);
        }
        break;
    }
    case LogicalOperatorKind::Sort: {
        const auto& sort = static_cast<const LogicalSort&>(node);
        stream << "Sort keys=[";
        bool first = true;
        for (const auto& key : sort.keys) {
            if (!first) {
                stream << ", ";
            }
            if (key.expression != nullptr) {
                stream << describe_expression(*key.expression);
            } else {
                stream << "<null>";
            }
            stream << (key.direction == OrderByItem::Direction::Descending ? " DESC" : " ASC");
            first = false;
        }
        stream << "]\n";
        if (sort.input != nullptr) {
            describe_plan(*sort.input, depth + 1U, stream);
        }
        break;
    }
    case LogicalOperatorKind::Limit: {
        const auto& limit = static_cast<const LogicalLimit&>(node);
        stream << "Limit";
        if (limit.row_count != nullptr) {
            stream << " row_count=" << describe_expression(*limit.row_count);
        }
        if (limit.offset != nullptr) {
            stream << " offset=" << describe_expression(*limit.offset);
        }
        stream << '\n';
        if (limit.input != nullptr) {
            describe_plan(*limit.input, depth + 1U, stream);
        }
        break;
    }
    case LogicalOperatorKind::Aggregate: {
        const auto& aggregate = static_cast<const LogicalAggregate&>(node);
        stream << "Aggregate";
        if (!aggregate.group_keys.empty()) {
            stream << " groups=[";
            bool first = true;
            for (const auto* key : aggregate.group_keys) {
                if (!first) {
                    stream << ", ";
                }
                if (key != nullptr) {
                    stream << describe_expression(*key);
                } else {
                    stream << "<null>";
                }
                first = false;
            }
            stream << ']';
        }
        stream << '\n';
        if (aggregate.input != nullptr) {
            describe_plan(*aggregate.input, depth + 1U, stream);
        }
        break;
    }
    default:
        stream << "<operator>\n";
        break;
    }
}

}  // namespace

std::string describe_plan(const LogicalOperator& root)
{
    std::ostringstream stream;
    describe_plan(root, 0U, stream);
    return stream.str();
}

}  // namespace bored::parser::relational
