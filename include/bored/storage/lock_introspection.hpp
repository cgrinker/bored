#pragma once

#include "bored/storage/lock_manager.hpp"

#include <functional>
#include <string>

namespace bored::storage {

inline constexpr std::uint32_t kLockSnapshotSchemaVersion = 1U;

struct LockDiagnosticsDocument final {
    std::uint32_t schema_version = kLockSnapshotSchemaVersion;
    std::vector<LockManager::LockSnapshot> locks;
};

using LockSnapshotSampler = std::function<std::vector<LockManager::LockSnapshot>()>;

void set_global_lock_snapshot_sampler(LockSnapshotSampler sampler) noexcept;
LockSnapshotSampler get_global_lock_snapshot_sampler() noexcept;
LockDiagnosticsDocument collect_global_lock_diagnostics();

std::string lock_diagnostics_to_json(const LockDiagnosticsDocument& document);

}  // namespace bored::storage
