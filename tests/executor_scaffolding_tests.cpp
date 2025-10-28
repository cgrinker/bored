#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_node.hpp"
#include "bored/executor/tuple_buffer.hpp"

#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <array>
#include <numeric>
#include <memory_resource>
#include <string>
#include <vector>

using bored::executor::ExecutorContext;
using bored::executor::ExecutorContextConfig;
using bored::executor::ExecutorNode;
using bored::executor::ExecutorNodePtr;
using bored::executor::TupleBuffer;
using bored::txn::Snapshot;
using Catch::Matchers::ContainsSubstring;

namespace {

class RecordingExecutor final : public ExecutorNode {
public:
    RecordingExecutor(std::vector<std::string>& events, std::string label)
        : events_{events}, label_{std::move(label)}
    {
    }

    void open(ExecutorContext& context) override
    {
        (void)context;
        events_.push_back(label_ + ":open");
        for (std::size_t index = 0; index < child_count(); ++index) {
            auto* child_ptr = child(index);
            REQUIRE(child_ptr != nullptr);
            child_ptr->open(context);
        }
    }

    bool next(ExecutorContext& context, TupleBuffer& buffer) override
    {
        events_.push_back(label_ + ":next");
        bool produced = false;
        for (std::size_t index = 0; index < child_count(); ++index) {
            auto* child_ptr = child(index);
            REQUIRE(child_ptr != nullptr);
            produced = child_ptr->next(context, buffer) || produced;
        }
        if (!produced && child_count() == 0U && !returned_once_) {
            buffer.allocate(sizeof(std::uint64_t));
            returned_once_ = true;
            return true;
        }
        return produced;
    }

    void close(ExecutorContext& context) override
    {
        events_.push_back(label_ + ":close");
        for (std::size_t index = child_count(); index > 0U; --index) {
            auto* child_ptr = child(index - 1U);
            REQUIRE(child_ptr != nullptr);
            child_ptr->close(context);
        }
    }

private:
    std::vector<std::string>& events_;
    std::string label_{};
    bool returned_once_ = false;
};

}  // namespace

TEST_CASE("ExecutorContext provides accessors")
{
    Snapshot snapshot{};
    snapshot.read_lsn = 4096U;
    snapshot.xmin = 10U;
    snapshot.xmax = 20U;

    std::byte buffer_storage[1024];
    std::pmr::monotonic_buffer_resource scratch_resource{buffer_storage, sizeof(buffer_storage)};

    ExecutorContextConfig config{};
    config.snapshot = snapshot;
    config.scratch = &scratch_resource;

    ExecutorContext context{config};
    CHECK(context.snapshot().read_lsn == 4096U);
    CHECK(context.scratch_resource() == &scratch_resource);

    context.set_transaction_id(777U);
    CHECK(context.transaction_id() == 777U);

    Snapshot updated{};
    updated.read_lsn = 8192U;
    context.set_snapshot(updated);
    CHECK(context.snapshot().read_lsn == 8192U);
}

TEST_CASE("TupleBuffer allocates aligned slices and resets")
{
    TupleBuffer buffer{64U};
    auto first = buffer.allocate(8U, 8U);
    REQUIRE(first.size() == 8U);
    REQUIRE(reinterpret_cast<std::uintptr_t>(first.data()) % 8U == 0U);

    std::array<std::byte, 6U> payload{};
    auto second = buffer.write(payload, 4U);
    REQUIRE(second.size() == payload.size());
    REQUIRE(reinterpret_cast<std::uintptr_t>(second.data()) % 4U == 0U);

    CHECK(buffer.size() >= first.size() + second.size());
    buffer.reset();
    CHECK(buffer.size() == 0U);

    auto third = buffer.allocate(32U, 16U);
    REQUIRE(reinterpret_cast<std::uintptr_t>(third.data()) % 16U == 0U);
    CHECK(buffer.size() == 32U);
}

TEST_CASE("Executor nodes propagate lifecycle events")
{
    std::vector<std::string> events;

    auto root = std::make_unique<RecordingExecutor>(events, "root");
    auto child = std::make_unique<RecordingExecutor>(events, "child");
    auto grandchild = std::make_unique<RecordingExecutor>(events, "leaf");

    child->add_child(std::move(grandchild));
    root->add_child(std::move(child));

    ExecutorContext context{};
    TupleBuffer buffer{};

    root->open(context);
    REQUIRE(root->next(context, buffer));
    root->close(context);

    const auto joined = std::accumulate(events.begin(), events.end(), std::string{}, [](const std::string& acc, const std::string& value) {
        if (acc.empty()) {
            return value;
        }
        return acc + "," + value;
    });

    CHECK_THAT(joined, ContainsSubstring("root:open"));
    CHECK_THAT(joined, ContainsSubstring("child:open"));
    CHECK_THAT(joined, ContainsSubstring("leaf:open"));
    CHECK_THAT(joined, ContainsSubstring("root:close"));
    CHECK(buffer.size() > 0U);
}
