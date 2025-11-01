#pragma once

#include <functional>
#include <system_error>

namespace bored::storage {

struct StorageCheckpointRequest final {
    bool force = true;
    bool dry_run = false;
};

struct StorageRetentionRequest final {
    bool include_index_retention = true;
};

struct StorageRecoveryRequest final {
    bool run_redo = true;
    bool run_undo = true;
    bool cleanup_only = false;
};

using StorageCheckpointHandler = std::function<std::error_code(const StorageCheckpointRequest&)>;
using StorageRetentionHandler = std::function<std::error_code(const StorageRetentionRequest&)>;
using StorageRecoveryHandler = std::function<std::error_code(const StorageRecoveryRequest&)>;

struct StorageControlHandlers final {
    StorageCheckpointHandler checkpoint;
    StorageRetentionHandler retention;
    StorageRecoveryHandler recovery;
};

StorageControlHandlers get_global_storage_control_handlers();
void set_global_storage_control_handlers(StorageControlHandlers handlers);
void reset_global_storage_control_handlers();

}  // namespace bored::storage
