#include "bored/storage/storage_control.hpp"

#include <mutex>
#include <utility>

namespace bored::storage {
namespace {
std::mutex g_mutex;
StorageControlHandlers g_handlers;
}  // namespace

StorageControlHandlers get_global_storage_control_handlers()
{
    std::lock_guard<std::mutex> guard{g_mutex};
    return g_handlers;
}

void set_global_storage_control_handlers(StorageControlHandlers handlers)
{
    std::lock_guard<std::mutex> guard{g_mutex};
    g_handlers = std::move(handlers);
}

void reset_global_storage_control_handlers()
{
    set_global_storage_control_handlers(StorageControlHandlers{});
}

}  // namespace bored::storage
