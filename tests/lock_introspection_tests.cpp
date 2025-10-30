#include "bored/storage/lock_introspection.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

TEST_CASE("Lock diagnostics JSON encodes holders")
{
    using namespace bored::storage;

    LockDiagnosticsDocument document{};
    LockManager::LockSnapshot snapshot{};
    snapshot.page_id = 90U;
    snapshot.total_shared = 2U;
    snapshot.exclusive_depth = 1U;
    snapshot.exclusive_owner = "thread-main";

    LockManager::LockHolderSnapshot holder{};
    holder.thread_id = "thread-main";
    holder.shared = 1U;
    holder.exclusive = 1U;
    snapshot.holders.push_back(holder);

    document.locks.push_back(snapshot);

    const auto json = lock_diagnostics_to_json(document);
    using Catch::Matchers::ContainsSubstring;
    CHECK_THAT(json, ContainsSubstring("\"locks\""));
    CHECK_THAT(json, ContainsSubstring("thread-main"));
}

TEST_CASE("Lock diagnostics sampler integrates with global registry")
{
    using namespace bored::storage;

    auto previous = get_global_lock_snapshot_sampler();
    set_global_lock_snapshot_sampler([] {
        std::vector<LockManager::LockSnapshot> locks;
        LockManager::LockSnapshot snapshot{};
        snapshot.page_id = 33U;
        locks.push_back(snapshot);
        return locks;
    });

    const auto document = collect_global_lock_diagnostics();
    REQUIRE(document.locks.size() == 1U);
    CHECK(document.locks.front().page_id == 33U);

    set_global_lock_snapshot_sampler(previous);
}
