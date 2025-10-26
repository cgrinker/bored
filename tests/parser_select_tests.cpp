#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/ast.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

namespace relational = bored::parser::relational;

TEST_CASE("parse_select handles basic select", "[parser][select]")
{
    const auto result = bored::parser::parse_select("SELECT name FROM inventory;");
    if (!result.diagnostics.empty()) {
        CAPTURE(result.diagnostics.front().message);
    }
    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.success());
    REQUIRE(result.statement != nullptr);

    const auto* query = result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->distinct == false);
    REQUIRE(query->select_items.size() == 1U);

    const auto* item = query->select_items.front();
    REQUIRE(item != nullptr);
    REQUIRE(item->expression != nullptr);
    REQUIRE(item->expression->kind == relational::NodeKind::IdentifierExpression);

    const auto& identifier = static_cast<const relational::IdentifierExpression&>(*item->expression);
    REQUIRE(identifier.name.parts.size() == 1U);
    REQUIRE(identifier.name.parts.front().value == "name");

    REQUIRE(query->from != nullptr);
    REQUIRE(query->from->name.parts.size() == 1U);
    REQUIRE(query->from->name.parts.front().value == "inventory");
    REQUIRE_FALSE(query->from->alias.has_value());
    REQUIRE(query->where == nullptr);
    REQUIRE(query->order_by.empty());
    REQUIRE(query->limit == nullptr);
}

TEST_CASE("parse_select supports distinct order and limit", "[parser][select]")
{
    const std::string base_plain = "SELECT DISTINCT * FROM inventory AS inv;";
    const auto base_plain_result = bored::parser::parse_select(base_plain);
    CAPTURE(base_plain);
    if (!base_plain_result.diagnostics.empty()) {
        CAPTURE(base_plain_result.diagnostics.front().message);
    }
    REQUIRE(base_plain_result.success());

    const std::string base_star = "SELECT DISTINCT inv.* FROM inventory AS inv;";
    const auto base_star_result = bored::parser::parse_select(base_star);
    CAPTURE(base_star);
    if (!base_star_result.diagnostics.empty()) {
        CAPTURE(base_star_result.diagnostics.front().message);
    }
    REQUIRE(base_star_result.success());

    const std::string base_schema = "SELECT DISTINCT inv.* FROM sales.inventory AS inv;";
    const auto base_schema_result = bored::parser::parse_select(base_schema);
    CAPTURE(base_schema);
    if (!base_schema_result.diagnostics.empty()) {
        CAPTURE(base_schema_result.diagnostics.front().message);
    }
    REQUIRE(base_schema_result.success());

    const std::string with_where =
        "SELECT DISTINCT inv.* FROM sales.inventory AS inv WHERE inv.id = 42;";
    const auto where_result = bored::parser::parse_select(with_where);
    CAPTURE(with_where);
    if (!where_result.diagnostics.empty()) {
        CAPTURE(where_result.diagnostics.front().message);
    }
    REQUIRE(where_result.success());

    const std::string with_order =
        "SELECT DISTINCT inv.* FROM sales.inventory AS inv ORDER BY inv.created_at DESC, inv.id ASC;";
    const auto order_result = bored::parser::parse_select(with_order);
    CAPTURE(with_order);
    if (!order_result.diagnostics.empty()) {
        CAPTURE(order_result.diagnostics.front().message);
    }
    REQUIRE(order_result.success());

    const std::string with_limit =
        "SELECT DISTINCT inv.* FROM sales.inventory AS inv LIMIT 10 OFFSET 5;";
    const auto limit_result = bored::parser::parse_select(with_limit);
    CAPTURE(with_limit);
    if (!limit_result.diagnostics.empty()) {
        CAPTURE(limit_result.diagnostics.front().message);
    }
    REQUIRE(limit_result.success());

    const std::string sql =
        "SELECT DISTINCT inv.*, inv.id AS item_id FROM sales.inventory AS inv "
        "WHERE inv.id = 42 ORDER BY inv.created_at DESC, inv.id ASC LIMIT 10 OFFSET 5;";

    const auto result = bored::parser::parse_select(sql);
    CAPTURE(sql);
    if (!result.diagnostics.empty()) {
        CAPTURE(result.diagnostics.front().message);
    }
    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.success());
    REQUIRE(result.statement != nullptr);

    const std::string description = bored::parser::relational::describe(*result.statement);
    INFO(description);
    CAPTURE(description);
    const auto* query = result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->distinct);
    REQUIRE(query->select_items.size() == 2U);

    const auto* first_item = query->select_items.front();
    REQUIRE(first_item != nullptr);
    REQUIRE(first_item->expression != nullptr);
    REQUIRE(first_item->expression->kind == relational::NodeKind::StarExpression);
    const auto& first_star = static_cast<const relational::StarExpression&>(*first_item->expression);
    REQUIRE(first_star.qualifier.parts.size() == 1U);
    REQUIRE(first_star.qualifier.parts.front().value == "inv");

    const auto* second_item = query->select_items.back();
    REQUIRE(second_item != nullptr);
    REQUIRE(second_item->alias.has_value());
    REQUIRE(second_item->alias->value == "item_id");
    REQUIRE(second_item->expression != nullptr);
    REQUIRE(second_item->expression->kind == relational::NodeKind::IdentifierExpression);

    REQUIRE(query->from != nullptr);
    REQUIRE(query->from->name.parts.size() == 2U);
    REQUIRE(query->from->name.parts.front().value == "sales");
    REQUIRE(query->from->name.parts.back().value == "inventory");
    REQUIRE(query->from->alias.has_value());
    REQUIRE(query->from->alias->value == "inv");

    REQUIRE(query->where != nullptr);
    REQUIRE(query->where->kind == relational::NodeKind::BinaryExpression);
    const auto& predicate = static_cast<const relational::BinaryExpression&>(*query->where);
    REQUIRE(predicate.op == relational::BinaryOperator::Equal);
    REQUIRE(predicate.left != nullptr);
    REQUIRE(predicate.left->kind == relational::NodeKind::IdentifierExpression);
    const auto& left_id = static_cast<const relational::IdentifierExpression&>(*predicate.left);
    REQUIRE(left_id.name.parts.size() == 2U);
    REQUIRE(left_id.name.parts.front().value == "inv");
    REQUIRE(left_id.name.parts.back().value == "id");
    REQUIRE(predicate.right != nullptr);
    REQUIRE(predicate.right->kind == relational::NodeKind::LiteralExpression);
    const auto& literal = static_cast<const relational::LiteralExpression&>(*predicate.right);
    REQUIRE(literal.tag == relational::LiteralTag::Integer);
    REQUIRE(literal.text == "42");

    REQUIRE(query->order_by.size() == 2U);
    const auto* first_order = query->order_by.front();
    REQUIRE(first_order != nullptr);
    REQUIRE(first_order->expression != nullptr);
    REQUIRE(first_order->expression->kind == relational::NodeKind::IdentifierExpression);
    REQUIRE(first_order->direction == relational::OrderByItem::Direction::Descending);

    const auto* limit = query->limit;
    REQUIRE(limit != nullptr);
    REQUIRE(limit->row_count != nullptr);
    REQUIRE(limit->row_count->kind == relational::NodeKind::LiteralExpression);
    const auto& limit_literal = static_cast<const relational::LiteralExpression&>(*limit->row_count);
    REQUIRE(limit_literal.tag == relational::LiteralTag::Integer);
    REQUIRE(limit_literal.text == "10");
    REQUIRE(limit->offset != nullptr);
    const auto& offset_literal = static_cast<const relational::LiteralExpression&>(*limit->offset);
    REQUIRE(offset_literal.text == "5");
}
