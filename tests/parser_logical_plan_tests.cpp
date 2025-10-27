#include "bored/parser/relational/logical_plan.hpp"

#include <catch2/catch_test_macros.hpp>

#include <vector>

namespace relational = bored::parser::relational;

namespace {

class RecordingVisitor final : public relational::LogicalOperatorVisitor {
public:
    void visit(const relational::LogicalScan&) override
    {
        kinds.push_back(relational::LogicalOperatorKind::Scan);
    }

    void visit(const relational::LogicalProject&) override
    {
        kinds.push_back(relational::LogicalOperatorKind::Project);
    }

    void visit(const relational::LogicalFilter&) override
    {
        kinds.push_back(relational::LogicalOperatorKind::Filter);
    }

    void visit(const relational::LogicalJoin&) override
    {
        kinds.push_back(relational::LogicalOperatorKind::Join);
    }

    void visit(const relational::LogicalAggregate&) override
    {
        kinds.push_back(relational::LogicalOperatorKind::Aggregate);
    }

    void visit(const relational::LogicalSort&) override
    {
        kinds.push_back(relational::LogicalOperatorKind::Sort);
    }

    void visit(const relational::LogicalLimit&) override
    {
        kinds.push_back(relational::LogicalOperatorKind::Limit);
    }

    std::vector<relational::LogicalOperatorKind> kinds{};
};

}  // namespace

TEST_CASE("logical operators expose output schema", "[parser][logical_plan]")
{
    relational::LogicalScan scan{};
    scan.output_schema.push_back(relational::LogicalColumn{"id", relational::ScalarType::Int64, false});
    scan.output_schema.push_back(relational::LogicalColumn{"name", relational::ScalarType::Utf8, true});

    REQUIRE(scan.output_schema.size() == 2U);
    CHECK(scan.output_schema.front().name == "id");
    CHECK(scan.output_schema.back().type == relational::ScalarType::Utf8);
}

TEST_CASE("logical operator visitor dispatches by kind", "[parser][logical_plan]")
{
    relational::LogicalLimit limit{};
    limit.kind = relational::LogicalOperatorKind::Limit;

    auto project = std::make_unique<relational::LogicalProject>();
    project->kind = relational::LogicalOperatorKind::Project;

    auto filter = std::make_unique<relational::LogicalFilter>();
    filter->kind = relational::LogicalOperatorKind::Filter;

    auto aggregate = std::make_unique<relational::LogicalAggregate>();
    aggregate->kind = relational::LogicalOperatorKind::Aggregate;

    auto sort = std::make_unique<relational::LogicalSort>();
    sort->kind = relational::LogicalOperatorKind::Sort;

    auto join = std::make_unique<relational::LogicalJoin>();
    join->kind = relational::LogicalOperatorKind::Join;

    auto scan = std::make_unique<relational::LogicalScan>();
    scan->kind = relational::LogicalOperatorKind::Scan;

    RecordingVisitor visitor{};
    scan->accept(visitor);
    project->accept(visitor);
    filter->accept(visitor);
    join->accept(visitor);
    aggregate->accept(visitor);
    sort->accept(visitor);
    limit.accept(visitor);

    REQUIRE(visitor.kinds.size() == 7U);
    CHECK(visitor.kinds[0] == relational::LogicalOperatorKind::Scan);
    CHECK(visitor.kinds[1] == relational::LogicalOperatorKind::Project);
    CHECK(visitor.kinds[2] == relational::LogicalOperatorKind::Filter);
    CHECK(visitor.kinds[3] == relational::LogicalOperatorKind::Join);
    CHECK(visitor.kinds[4] == relational::LogicalOperatorKind::Aggregate);
    CHECK(visitor.kinds[5] == relational::LogicalOperatorKind::Sort);
    CHECK(visitor.kinds[6] == relational::LogicalOperatorKind::Limit);
}
