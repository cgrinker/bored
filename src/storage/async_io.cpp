#include "bored/storage/async_io.hpp"

#include <algorithm>
#include <atomic>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#if defined(__APPLE__)
#    include <dispatch/dispatch.h>
#    include <fcntl.h>
#    include <sys/stat.h>
#    include <sys/types.h>
#    include <unistd.h>
#endif

#if defined(_WIN32)
#    ifndef NOMINMAX
#        define NOMINMAX
#    endif
#    define WIN32_LEAN_AND_MEAN
#    include <windows.h>
#    if defined(__has_include)
#        if __has_include(<ioringapi.h>)
#            include <ioringapi.h>
#            define BORED_STORAGE_HAVE_IORING 1
#        else
#            define BORED_STORAGE_HAVE_IORING 0
#        endif
#    else
#        define BORED_STORAGE_HAVE_IORING 0
#    endif
#else
#    define BORED_STORAGE_HAVE_IORING 0
#endif

namespace bored::storage {

namespace {

constexpr std::size_t kFileClassCount = static_cast<std::size_t>(FileClass::WriteAheadLog) + 1U;
static_assert(kFileClassCount >= 2U, "Unexpected FileClass enumeration ordering");

IoResult perform_read(const ReadRequest& request)
{
    IoResult result{};

    if (request.data == nullptr || request.size == 0U) {
        result.status = std::make_error_code(std::errc::invalid_argument);
        return result;
    }

    std::ifstream stream(request.path, std::ios::binary);
    if (!stream) {
        result.status = std::make_error_code(std::errc::no_such_file_or_directory);
        return result;
    }

    stream.seekg(static_cast<std::streamoff>(request.offset));
    if (!stream) {
        result.status = std::make_error_code(std::errc::invalid_argument);
        return result;
    }

    stream.read(reinterpret_cast<char*>(request.data), static_cast<std::streamsize>(request.size));
    const auto read_count = stream.gcount();
    if (read_count < 0) {
        result.status = std::make_error_code(std::errc::io_error);
        return result;
    }

    result.bytes_transferred = static_cast<std::size_t>(read_count);
    return result;
}

IoResult perform_write(const WriteRequest& request)
{
    IoResult result{};

    if (request.data == nullptr || request.size == 0U) {
        result.status = std::make_error_code(std::errc::invalid_argument);
        return result;
    }

    std::fstream stream(request.path, std::ios::binary | std::ios::in | std::ios::out);
    if (!stream) {
        stream.open(request.path, std::ios::binary | std::ios::out | std::ios::trunc);
        stream.close();
        stream.open(request.path, std::ios::binary | std::ios::in | std::ios::out);
    }

    if (!stream) {
        result.status = std::make_error_code(std::errc::io_error);
        return result;
    }

    stream.seekp(static_cast<std::streamoff>(request.offset));
    if (!stream) {
        result.status = std::make_error_code(std::errc::invalid_argument);
        return result;
    }

    stream.write(reinterpret_cast<const char*>(request.data), static_cast<std::streamsize>(request.size));
    if (!stream) {
        result.status = std::make_error_code(std::errc::io_error);
        return result;
    }

    if (any(request.flags & IoFlag::Dsync)) {
        stream.flush();
    }

    result.bytes_transferred = request.size;
    return result;
}

class ThreadPoolAsyncIo final : public AsyncIo {
public:
    explicit ThreadPoolAsyncIo(const AsyncIoConfig& config)
        : config_{config}
    {
        const auto worker_count = std::max<std::size_t>(1U, config_.worker_threads);
        for (std::size_t index = 0; index < worker_count; ++index) {
            workers_.emplace_back([this]() { worker_loop(); });
        }
    }

    ~ThreadPoolAsyncIo() override
    {
        shutdown();
    }

    std::future<IoResult> submit_read(ReadRequest request) override
    {
        Operation operation{[request = std::move(request)]() mutable -> IoResult {
            return perform_read(request);
        }};
        return enqueue(std::move(operation));
    }

    std::future<IoResult> submit_write(WriteRequest request) override
    {
        Operation operation{[request = std::move(request)]() mutable -> IoResult {
            return perform_write(request);
        }};
        return enqueue(std::move(operation));
    }

    std::future<IoResult> flush(FileClass) override
    {
        return std::async(std::launch::async, [this]() {
            std::unique_lock lock(inflight_mutex_);
            inflight_cv_.wait(lock, [this]() { return inflight_operations_ == 0U; });
            return IoResult{};
        });
    }

    void shutdown() override
    {
        {
            std::scoped_lock lock(queue_mutex_);
            if (!running_) {
                return;
            }
            running_ = false;
        }
        queue_cv_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    using Operation = std::packaged_task<IoResult()>;

    std::future<IoResult> enqueue(Operation operation)
    {
        std::unique_lock lock(queue_mutex_);
        queue_cv_.wait(lock, [this]() { return queue_.size() < config_.queue_depth || !running_; });
        if (!running_) {
            lock.unlock();
            std::promise<IoResult> promise;
            auto future = promise.get_future();
            promise.set_value(IoResult{0U, std::make_error_code(std::errc::operation_canceled)});
            return future;
        }

        auto future = operation.get_future();
        queue_.emplace_back(std::move(operation));
        lock.unlock();
        queue_cv_.notify_one();
        return future;
    }

    void worker_loop()
    {
        while (true) {
            Operation operation;
            {
                std::unique_lock lock(queue_mutex_);
                queue_cv_.wait(lock, [this]() { return !queue_.empty() || !running_; });
                if (!running_ && queue_.empty()) {
                    return;
                }
                operation = std::move(queue_.front());
                queue_.pop_front();
            }

            {
                std::scoped_lock lock(inflight_mutex_);
                ++inflight_operations_;
            }

            operation();

            {
                std::scoped_lock lock(inflight_mutex_);
                --inflight_operations_;
            }
            inflight_cv_.notify_all();
        }
    }

    AsyncIoConfig config_{};
    std::vector<std::thread> workers_{};
    std::deque<Operation> queue_{};
    std::mutex queue_mutex_{};
    std::condition_variable queue_cv_{};
    bool running_ = true;

    std::mutex inflight_mutex_{};
    std::condition_variable inflight_cv_{};
    std::size_t inflight_operations_ = 0U;
};

#if defined(__APPLE__)

class DispatchAsyncIo final : public AsyncIo {
public:
    explicit DispatchAsyncIo(const AsyncIoConfig& config)
        : config_{config}
    {
        const auto depth = std::max<std::size_t>(1U, config_.queue_depth);
        queue_ = dispatch_queue_create("bored.storage.asyncio.dispatch", DISPATCH_QUEUE_CONCURRENT);
        slots_ = dispatch_semaphore_create(static_cast<long>(depth));
        if (queue_ == nullptr || slots_ == nullptr) {
            if (queue_ != nullptr) {
                dispatch_release(queue_);
                queue_ = nullptr;
            }
            if (slots_ != nullptr) {
                dispatch_release(slots_);
                slots_ = nullptr;
            }
            throw std::system_error(std::make_error_code(std::errc::not_enough_memory),
                                    "Failed to create dispatch async IO resources");
        }
        running_.store(true, std::memory_order_release);
    }

    ~DispatchAsyncIo() override
    {
        shutdown();
        if (queue_ != nullptr) {
            dispatch_release(queue_);
            queue_ = nullptr;
        }
        if (slots_ != nullptr) {
            dispatch_release(slots_);
            slots_ = nullptr;
        }
    }

    std::future<IoResult> submit_read(ReadRequest request) override
    {
        if (request.data == nullptr || request.size == 0U) {
            return invalid_argument_future();
        }
        return enqueue<ReadRequest, DispatchReadContext, &DispatchAsyncIo::dispatch_read_trampoline>(std::move(request));
    }

    std::future<IoResult> submit_write(WriteRequest request) override
    {
        if (request.data == nullptr || request.size == 0U) {
            return invalid_argument_future();
        }
        return enqueue<WriteRequest, DispatchWriteContext, &DispatchAsyncIo::dispatch_write_trampoline>(std::move(request));
    }

    std::future<IoResult> flush(FileClass file_class) override
    {
        return std::async(std::launch::async, [this, file_class]() {
            IoResult result{};
            std::unique_lock lock(inflight_mutex_);
            const auto index = class_index(file_class);
            auto ready = [this, index]() {
                const auto running = running_.load(std::memory_order_acquire);
                if (index >= inflight_by_class_.size()) {
                    return true;
                }
                return inflight_by_class_[index] == 0U || !running;
            };

            if (config_.shutdown_timeout.count() > 0) {
                if (!inflight_cv_.wait_for(lock, config_.shutdown_timeout, ready)) {
                    result.status = std::make_error_code(std::errc::timed_out);
                    return result;
                }
            } else {
                inflight_cv_.wait(lock, ready);
            }

            if (!running_.load(std::memory_order_acquire) && index < inflight_by_class_.size()
                && inflight_by_class_[index] != 0U) {
                result.status = std::make_error_code(std::errc::operation_canceled);
            }
            return result;
        });
    }

    void shutdown() override
    {
        bool expected = true;
        if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            return;
        }

        inflight_cv_.notify_all();

        if (queue_ != nullptr) {
            dispatch_barrier_sync_f(queue_, nullptr, &DispatchAsyncIo::barrier_noop);
        }

        std::unique_lock lock(inflight_mutex_);
        if (config_.shutdown_timeout.count() > 0) {
            inflight_cv_.wait_for(lock, config_.shutdown_timeout, [this]() {
                return inflight_total_ == 0U;
            });
        } else {
            inflight_cv_.wait(lock, [this]() {
                return inflight_total_ == 0U;
            });
        }
    }

private:
    struct DispatchOperationBase {
        DispatchAsyncIo* self = nullptr;
        std::shared_ptr<std::promise<IoResult>> promise{};
        std::size_t class_index = 0U;
    };

    struct DispatchReadContext : DispatchOperationBase {
        ReadRequest request{};
    };

    struct DispatchWriteContext : DispatchOperationBase {
        WriteRequest request{};
    };

    template <typename Request, typename Context, void (*Trampoline)(void*)>
    std::future<IoResult> enqueue(Request request)
    {
        if (!running_.load(std::memory_order_acquire)) {
            return cancelled_future();
        }

        if (slots_ != nullptr) {
            dispatch_semaphore_wait(slots_, DISPATCH_TIME_FOREVER);
            if (!running_.load(std::memory_order_acquire)) {
                dispatch_semaphore_signal(slots_);
                return cancelled_future();
            }
        }

        auto promise = std::make_shared<std::promise<IoResult>>();
        auto future = promise->get_future();
        const auto index = class_index(request.file_class);

        {
            std::lock_guard guard(inflight_mutex_);
            ++inflight_total_;
            if (index < inflight_by_class_.size()) {
                ++inflight_by_class_[index];
            }
        }

        auto* context = new Context{};
        context->self = this;
        context->promise = std::move(promise);
        context->class_index = index;
        context->request = std::move(request);

        dispatch_async_f(queue_, context, Trampoline);

        return future;
    }

    static void dispatch_read_trampoline(void* raw)
    {
        auto* context = static_cast<DispatchReadContext*>(raw);
        context->self->process_read(*context);
        delete context;
    }

    static void dispatch_write_trampoline(void* raw)
    {
        auto* context = static_cast<DispatchWriteContext*>(raw);
        context->self->process_write(*context);
        delete context;
    }

    static void barrier_noop(void*) {}

    void process_read(DispatchReadContext& context)
    {
        auto result = perform_read_request(context.request);
        complete_operation(context.class_index, std::move(context.promise), result);
    }

    void process_write(DispatchWriteContext& context)
    {
        auto result = perform_write_request(context.request);
        complete_operation(context.class_index, std::move(context.promise), result);
    }

    IoResult perform_read_request(const ReadRequest& request)
    {
        IoResult result{};
        if (request.data == nullptr || request.size == 0U) {
            result.status = std::make_error_code(std::errc::invalid_argument);
            return result;
        }

        const auto path_native = request.path.native();
        const int fd = open(path_native.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            const int open_error = errno;
            result.status = std::error_code(open_error, std::generic_category());
            return result;
        }

        std::size_t total_read = 0U;
        std::size_t remaining = request.size;
        const auto base_offset = static_cast<off_t>(request.offset);

        while (remaining > 0U) {
            const auto current_offset = base_offset + static_cast<off_t>(total_read);
            const auto chunk = std::min<std::size_t>(remaining, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
            const ssize_t read_count = pread(fd, request.data + total_read, chunk, current_offset);
            if (read_count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                result.status = std::error_code(errno, std::generic_category());
                break;
            }
            if (read_count == 0) {
                break;
            }
            total_read += static_cast<std::size_t>(read_count);
            remaining -= static_cast<std::size_t>(read_count);
        }

        if (close(fd) != 0 && !result.status) {
            const int close_error = errno;
            result.status = std::error_code(close_error, std::generic_category());
        }

        result.bytes_transferred = total_read;
        return result;
    }

    IoResult perform_write_request(const WriteRequest& request)
    {
        IoResult result{};
        if (request.data == nullptr || request.size == 0U) {
            result.status = std::make_error_code(std::errc::invalid_argument);
            return result;
        }

        const auto path_native = request.path.native();
        const int fd = open(path_native.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        if (fd < 0) {
            const int open_error = errno;
            result.status = std::error_code(open_error, std::generic_category());
            return result;
        }

        std::size_t total_written = 0U;
        std::size_t remaining = request.size;
        const auto base_offset = static_cast<off_t>(request.offset);

        while (remaining > 0U) {
            const auto current_offset = base_offset + static_cast<off_t>(total_written);
            const auto chunk = std::min<std::size_t>(remaining, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
            const ssize_t write_count = pwrite(fd, request.data + total_written, chunk, current_offset);
            if (write_count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                result.status = std::error_code(errno, std::generic_category());
                break;
            }
            total_written += static_cast<std::size_t>(write_count);
            remaining -= static_cast<std::size_t>(write_count);
        }

        if (!result.status && any(request.flags & IoFlag::Dsync)) {
            if (config_.use_full_fsync) {
                if (fcntl(fd, F_FULLFSYNC) != 0) {
                    const int sync_error = errno;
                    result.status = std::error_code(sync_error, std::generic_category());
                }
            } else {
                if (fsync(fd) != 0) {
                    const int sync_error = errno;
                    result.status = std::error_code(sync_error, std::generic_category());
                }
            }
        }

        if (close(fd) != 0 && !result.status) {
            const int close_error = errno;
            result.status = std::error_code(close_error, std::generic_category());
        }

        result.bytes_transferred = total_written;
        return result;
    }

    void complete_operation(std::size_t class_index, std::shared_ptr<std::promise<IoResult>> promise, IoResult result)
    {
        if (promise) {
            promise->set_value(result);
        }

        if (slots_ != nullptr) {
            dispatch_semaphore_signal(slots_);
        }

        {
            std::lock_guard guard(inflight_mutex_);
            if (inflight_total_ > 0U) {
                --inflight_total_;
            }
            if (class_index < inflight_by_class_.size() && inflight_by_class_[class_index] > 0U) {
                --inflight_by_class_[class_index];
            }
        }
        inflight_cv_.notify_all();
    }

    static std::future<IoResult> invalid_argument_future()
    {
        std::promise<IoResult> promise;
        auto future = promise.get_future();
        IoResult result{};
        result.status = std::make_error_code(std::errc::invalid_argument);
        promise.set_value(result);
        return future;
    }

    static std::future<IoResult> cancelled_future()
    {
        std::promise<IoResult> promise;
        auto future = promise.get_future();
        IoResult result{};
        result.status = std::make_error_code(std::errc::operation_canceled);
        promise.set_value(result);
        return future;
    }

    static constexpr std::size_t class_index(FileClass file_class)
    {
        return static_cast<std::size_t>(file_class);
    }

    AsyncIoConfig config_{};
    dispatch_queue_t queue_ = nullptr;
    dispatch_semaphore_t slots_ = nullptr;
    std::atomic<bool> running_{false};

    std::mutex inflight_mutex_{};
    std::condition_variable inflight_cv_{};
    std::array<std::size_t, kFileClassCount> inflight_by_class_{};
    std::size_t inflight_total_ = 0U;
};

#endif  // defined(__APPLE__)

#if BORED_STORAGE_HAVE_IORING

using PopIoRingCompletionExFn = HRESULT(WINAPI*)(HIORING, IORING_CQE*, ULONG, ULONG, PULONG);

PopIoRingCompletionExFn load_pop_completion_ex()
{
    static PopIoRingCompletionExFn cached = []() -> PopIoRingCompletionExFn {
        const HMODULE module = GetModuleHandleW(L"kernel32.dll");
        if (!module) {
            return nullptr;
        }
        return reinterpret_cast<PopIoRingCompletionExFn>(GetProcAddress(module, "PopIoRingCompletionEx"));
    }();
    return cached;
}

class IoRingDispatcher final : public AsyncIo {
public:
    explicit IoRingDispatcher(const AsyncIoConfig& config)
        : config_{config}
        , queue_capacity_{std::max<std::size_t>(1U, std::min<std::size_t>(config.queue_depth, static_cast<std::size_t>(std::numeric_limits<ULONG>::max())))}
    {
        const ULONG queue_depth = static_cast<ULONG>(queue_capacity_);
        IORING_CREATE_FLAGS flags{};
        const auto hr = CreateIoRing(IORING_VERSION_3, flags, queue_depth, queue_depth, &ring_);
        if (FAILED(hr)) {
            throw std::system_error(static_cast<int>(HRESULT_CODE(hr)), std::system_category(), "CreateIoRing failed");
        }

        completion_event_ = CreateEventW(nullptr, FALSE, FALSE, nullptr);
        if (completion_event_ != nullptr) {
            const auto event_hr = SetIoRingCompletionEvent(ring_, completion_event_);
            if (FAILED(event_hr)) {
                CloseHandle(completion_event_);
                completion_event_ = nullptr;
            }
        }

        running_.store(true, std::memory_order_release);
        completion_thread_ = std::thread([this]() { completion_loop(); });
    }

    ~IoRingDispatcher() override
    {
        shutdown();
    }

    std::future<IoResult> submit_read(ReadRequest request) override
    {
        return submit_operation(std::move(request));
    }

    std::future<IoResult> submit_write(WriteRequest request) override
    {
        return submit_operation(std::move(request));
    }

    std::future<IoResult> flush(FileClass file_class) override
    {
        return std::async(std::launch::async, [this, file_class]() {
            submit_ring();
            std::unique_lock lock(operation_mutex_);
            const auto index = class_index(file_class);
            if (index >= inflight_by_class_.size()) {
                IoResult result{};
                if (const auto fatal = fatal_error_.load(std::memory_order_acquire); fatal != 0) {
                    result.status = std::error_code(fatal, std::system_category());
                }
                return result;
            }

            operation_cv_.wait(lock, [this, index]() {
                return inflight_by_class_[index] == 0U || !running_.load(std::memory_order_acquire)
                       || fatal_error_.load(std::memory_order_acquire) != 0;
            });

            IoResult result{};
            if (const auto fatal = fatal_error_.load(std::memory_order_acquire); fatal != 0) {
                result.status = std::error_code(fatal, std::system_category());
            } else if (!running_.load(std::memory_order_acquire) && inflight_by_class_[index] != 0U) {
                result.status = std::make_error_code(std::errc::operation_canceled);
            }
            return result;
        });
    }

    void shutdown() override
    {
        std::cout << "[ioring-dispatcher] shutdown begin" << std::endl;
        bool expected = true;
        if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            std::cout << "[ioring-dispatcher] shutdown already requested" << std::endl;
            return;
        }

        operation_cv_.notify_all();
        const auto submit_hr = SubmitIoRing(ring_, 0, 0, nullptr);
        if (FAILED(submit_hr)) {
            handle_ring_failure(submit_hr);
            std::cout << "[ioring-dispatcher] SubmitIoRing failed hr=" << std::hex << submit_hr << std::dec << std::endl;
        }

        if (completion_event_ != nullptr) {
            SetEvent(completion_event_);
        }

        if (completion_thread_.joinable()) {
#if defined(_WIN32)
            if (auto native = completion_thread_.native_handle(); native != nullptr) {
                if (CancelSynchronousIo(static_cast<HANDLE>(native)) == 0) {
                    std::cout << "[ioring-dispatcher] CancelSynchronousIo failed error=" << GetLastError() << std::endl;
                } else {
                    std::cout << "[ioring-dispatcher] CancelSynchronousIo succeeded" << std::endl;
                }
            }
#endif
            completion_thread_.join();
            std::cout << "[ioring-dispatcher] completion thread joined" << std::endl;
        }

        std::unordered_map<ULONG_PTR, std::unique_ptr<Operation>> remaining;
        {
            std::scoped_lock lock(operation_mutex_);
            remaining.swap(operations_);
            inflight_by_class_.fill(0U);
        }
        operation_cv_.notify_all();

        const auto fatal = fatal_error_.load(std::memory_order_acquire);
        std::error_code cancel_status = fatal != 0 ? std::error_code(fatal, std::system_category())
                                                  : std::make_error_code(std::errc::operation_canceled);

        std::cout << "[ioring-dispatcher] remaining operations=" << remaining.size() << std::endl;
        for (auto& [token, operation] : remaining) {
            (void)token;
            if (operation->file != INVALID_HANDLE_VALUE) {
                CancelIoEx(operation->file, nullptr);
                CloseHandle(operation->file);
            }
            IoResult result{};
            result.status = cancel_status;
            operation->promise.set_value(result);
        }
        std::cout << "[ioring-dispatcher] shutdown complete" << std::endl;

        destroy_ring();
        if (ring_closed_) {
            ring_ = nullptr;
        }
    }

private:
    void destroy_ring()
    {
        if (ring_ != nullptr && !ring_closed_) {
            CloseIoRing(ring_);
            ring_closed_ = true;
        }
        if (completion_event_ != nullptr) {
            CloseHandle(completion_event_);
            completion_event_ = nullptr;
        }
    }

    struct Operation {
        std::promise<IoResult> promise{};
        HANDLE file = INVALID_HANDLE_VALUE;
        std::uint64_t offset = 0U;
        FileClass file_class = FileClass::Data;
        IoFlag flags = IoFlag::None;
        std::byte* read_buffer = nullptr;
        const std::byte* write_buffer = nullptr;
        std::size_t size = 0U;
        bool is_write = false;
        ULONG_PTR token = 0U;
    };

    static constexpr std::size_t kFileClassCount = static_cast<std::size_t>(FileClass::WriteAheadLog) + 1U;
    static_assert(kFileClassCount >= 2U, "Unexpected FileClass enumeration ordering");

    template <typename Request>
    std::future<IoResult> submit_operation(Request request)
    {
        static_assert(std::is_same_v<Request, ReadRequest> || std::is_same_v<Request, WriteRequest>, "Unsupported request type");

        if (const auto fatal = fatal_error_.load(std::memory_order_acquire); fatal != 0) {
            return error_future(std::error_code(fatal, std::system_category()));
        }

        if (!running_.load(std::memory_order_acquire)) {
            return cancelled_future();
        }

        if constexpr (std::is_same_v<Request, ReadRequest>) {
            if (request.data == nullptr || request.size == 0U) {
                return invalid_argument_future();
            }
        } else {
            if (request.data == nullptr || request.size == 0U) {
                return invalid_argument_future();
            }
        }

        const auto handle = open_file(request);
        if (handle == INVALID_HANDLE_VALUE) {
            const auto error = GetLastError();
            return error_future(std::error_code(static_cast<int>(error), std::system_category()));
        }

        auto operation = std::make_unique<Operation>();
        operation->file = handle;
        operation->offset = request.offset;
        operation->file_class = request.file_class;
        operation->is_write = std::is_same_v<Request, WriteRequest>;
        operation->size = request.size;

        if constexpr (std::is_same_v<Request, ReadRequest>) {
            operation->read_buffer = request.data;
            operation->flags = IoFlag::None;
        } else {
            operation->write_buffer = request.data;
            operation->flags = request.flags;
        }

        auto future = operation->promise.get_future();

        std::unique_lock lock(operation_mutex_);
        operation_cv_.wait(lock, [this]() {
            return operations_.size() < queue_capacity_ || !running_.load(std::memory_order_acquire)
                   || fatal_error_.load(std::memory_order_acquire) != 0;
        });

        const auto fatal = fatal_error_.load(std::memory_order_acquire);
        if (!running_.load(std::memory_order_acquire) || fatal != 0) {
            lock.unlock();
            CloseHandle(handle);
            IoResult result{};
            if (fatal != 0) {
                result.status = std::error_code(fatal, std::system_category());
            } else {
                result.status = std::make_error_code(std::errc::operation_canceled);
            }
            operation->promise.set_value(result);
            return future;
        }

        const auto token = next_token_.fetch_add(1U, std::memory_order_relaxed);
        operation->token = token;

        const auto index = class_index(operation->file_class);
        if (index < inflight_by_class_.size()) {
            ++inflight_by_class_[index];
        }

        operations_.emplace(token, std::move(operation));
        lock.unlock();

        const auto hr = submit_to_ring(token);
        if (FAILED(hr)) {
            complete_with_error(token, hr);
        } else {
            submit_ring();
        }

        return future;
    }

    static std::future<IoResult> invalid_argument_future()
    {
        std::promise<IoResult> promise;
        auto future = promise.get_future();
        promise.set_value(IoResult{0U, std::make_error_code(std::errc::invalid_argument)});
        return future;
    }

    static std::future<IoResult> cancelled_future()
    {
        std::promise<IoResult> promise;
        auto future = promise.get_future();
        promise.set_value(IoResult{0U, std::make_error_code(std::errc::operation_canceled)});
        return future;
    }

    static std::future<IoResult> error_future(std::error_code error)
    {
        std::promise<IoResult> promise;
        auto future = promise.get_future();
        promise.set_value(IoResult{0U, error});
        return future;
    }

    template <typename Request>
    HANDLE open_file(const Request& request)
    {
        const auto wide = request.path.wstring();
        const DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
        const DWORD flags = FILE_FLAG_OVERLAPPED | FILE_ATTRIBUTE_NORMAL;
        if constexpr (std::is_same_v<Request, ReadRequest>) {
            return CreateFileW(wide.c_str(), GENERIC_READ, share_mode, nullptr, OPEN_EXISTING, flags, nullptr);
        } else {
            const DWORD creation = OPEN_ALWAYS;
            return CreateFileW(wide.c_str(), GENERIC_WRITE, share_mode, nullptr, creation, flags, nullptr);
        }
    }

    HRESULT submit_to_ring(ULONG_PTR token)
    {
        Operation* operation = nullptr;
        {
            std::scoped_lock lock(operation_mutex_);
            auto iter = operations_.find(token);
            if (iter == operations_.end()) {
                return HRESULT_FROM_WIN32(ERROR_NOT_FOUND);
            }
            operation = iter->second.get();
        }

        if (operation->size > std::numeric_limits<ULONG>::max()) {
            return HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
        }

        const auto handle_ref = IoRingHandleRefFromHandle(operation->file);
        const auto length = static_cast<ULONG>(operation->size);

        HRESULT hr = S_OK;

        if (operation->is_write) {
            auto buffer_ref = IoRingBufferRefFromPointer(const_cast<std::byte*>(operation->write_buffer));
    // Windows IoRing rejects write-through flags on cached handles; defer to FlushFileBuffers.
    const auto write_flags = FILE_WRITE_FLAGS_NONE;
        hr = BuildIoRingWriteFile(ring_, handle_ref, buffer_ref, length, operation->offset, write_flags, operation->token, IOSQE_FLAGS_NONE);
        } else {
            auto buffer_ref = IoRingBufferRefFromPointer(operation->read_buffer);
            hr = BuildIoRingReadFile(ring_, handle_ref, buffer_ref, length, operation->offset, operation->token, IOSQE_FLAGS_NONE);
        }

#if defined(IORING_E_SUBMISSION_QUEUE_FULL) || defined(IORING_E_COMPLETION_QUEUE_FULL)
    if (
#    if defined(IORING_E_SUBMISSION_QUEUE_FULL)
        hr == IORING_E_SUBMISSION_QUEUE_FULL
#    endif
#    if defined(IORING_E_SUBMISSION_QUEUE_FULL) && defined(IORING_E_COMPLETION_QUEUE_FULL)
        ||
#    endif
#    if defined(IORING_E_COMPLETION_QUEUE_FULL)
        hr == IORING_E_COMPLETION_QUEUE_FULL
#    endif
    ) {
            if (!submit_ring()) {
                return HRESULT_FROM_WIN32(ERROR_OPERATION_ABORTED);
            }
            std::this_thread::yield();
            if (fatal_error_.load(std::memory_order_acquire) != 0) {
                return HRESULT_FROM_WIN32(ERROR_OPERATION_ABORTED);
            }
            if (operation->is_write) {
                auto buffer_ref = IoRingBufferRefFromPointer(const_cast<std::byte*>(operation->write_buffer));
                const auto write_flags = FILE_WRITE_FLAGS_NONE;
                hr = BuildIoRingWriteFile(ring_, handle_ref, buffer_ref, length, operation->offset, write_flags, operation->token, IOSQE_FLAGS_NONE);
            } else {
                auto buffer_ref = IoRingBufferRefFromPointer(operation->read_buffer);
                hr = BuildIoRingReadFile(ring_, handle_ref, buffer_ref, length, operation->offset, operation->token, IOSQE_FLAGS_NONE);
            }
        }
#endif

        return hr;
    }

    void complete_with_error(ULONG_PTR token, HRESULT hr)
    {
        std::unique_ptr<Operation> cleanup;
        {
            std::scoped_lock lock(operation_mutex_);
            auto iter = operations_.find(token);
            if (iter != operations_.end()) {
                cleanup = std::move(iter->second);
                decrement_inflight_locked(cleanup->file_class);
                operations_.erase(iter);
            }
        }
        operation_cv_.notify_all();

        if (!cleanup) {
            return;
        }

        if (cleanup->file != INVALID_HANDLE_VALUE) {
            CloseHandle(cleanup->file);
        }

        IoResult result{};
        result.status = std::error_code(static_cast<int>(HRESULT_CODE(hr)), std::system_category());
        cleanup->promise.set_value(result);
    }

    bool submit_ring()
    {
        const auto hr = SubmitIoRing(ring_, 0, 0, nullptr);
        if (FAILED(hr)) {
            handle_ring_failure(hr);
            return false;
        }
        return true;
    }

    void handle_ring_failure(HRESULT hr)
    {
        const auto code = static_cast<int>(HRESULT_CODE(hr));
        if (code == 0) {
            return;
        }

        int expected = 0;
        if (!fatal_error_.compare_exchange_strong(expected, code, std::memory_order_acq_rel)) {
            return;
        }

        std::cout << "[ioring-dispatcher] fatal error hr=" << std::hex << hr << std::dec << " code=" << code << std::endl;

        running_.store(false, std::memory_order_release);

        std::unordered_map<ULONG_PTR, std::unique_ptr<Operation>> pending;
        {
            std::scoped_lock lock(operation_mutex_);
            pending.swap(operations_);
            inflight_by_class_.fill(0U);
        }
        operation_cv_.notify_all();

        const std::error_code error{code, std::system_category()};
        for (auto& [token, operation] : pending) {
            (void)token;
            if (operation->file != INVALID_HANDLE_VALUE) {
                CancelIoEx(operation->file, nullptr);
                CloseHandle(operation->file);
            }
            IoResult result{};
            result.status = error;
            operation->promise.set_value(result);
        }
    }

    void completion_loop()
    {
        std::cout << "[ioring-dispatcher] completion loop start" << std::endl;
        while (running_.load(std::memory_order_acquire)) {
            if (!drain_completions()) {
                std::cout << "[ioring-dispatcher] drain_completions signaled stop" << std::endl;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }

        if (fatal_error_.load(std::memory_order_acquire) == 0) {
            drain_completions();
        }
        std::cout << "[ioring-dispatcher] completion loop exit" << std::endl;
    }

    bool drain_completions()
    {
        static thread_local std::uint32_t sentinel_streak = 0;
        IORING_CQE completion{};
        while (true) {
            if (auto pop_ex = load_pop_completion_ex()) {
                ULONG popped = 0;
                const auto hr = pop_ex(ring_, &completion, 1, 0, &popped);
                if (FAILED(hr)) {
                    if (hr == HRESULT_FROM_WIN32(ERROR_NO_MORE_ITEMS) || hr == HRESULT_FROM_WIN32(WAIT_TIMEOUT)) {
                        std::cout << "[ioring-dispatcher] no more completions hr=" << std::hex << hr << std::dec
                                  << " running=" << running_.load(std::memory_order_acquire) << std::endl;
                        return true;
                    }
                    std::cout << "[ioring-dispatcher] PopIoRingCompletionEx failed hr=" << std::hex << hr << std::dec
                              << std::endl;
                    handle_ring_failure(hr);
                    return false;
                }

                if (popped == 0) {
                    if (!running_.load(std::memory_order_acquire)) {
                        std::cout << "[ioring-dispatcher] PopIoRingCompletionEx yielded 0 entries while stopping"
                                  << std::endl;
                        return true;
                    }
                    std::cout << "[ioring-dispatcher] PopIoRingCompletionEx yielded 0 entries" << std::endl;
                    std::this_thread::yield();
                    continue;
                }
            } else {
                const auto hr = PopIoRingCompletion(ring_, &completion);
                if (FAILED(hr)) {
                    if (hr == HRESULT_FROM_WIN32(ERROR_NO_MORE_ITEMS)) {
                        std::cout << "[ioring-dispatcher] no more completions hr=" << std::hex << hr << std::dec
                                  << " running=" << running_.load(std::memory_order_acquire) << std::endl;
                        return true;
                    }
                    std::cout << "[ioring-dispatcher] PopIoRingCompletion failed hr=" << std::hex << hr << std::dec
                              << std::endl;
                    handle_ring_failure(hr);
                    return false;
                }
            }
            if (completion.UserData == 0) {
                ++sentinel_streak;
                if (sentinel_streak <= 3 || (sentinel_streak % 5000U) == 0U) {
                    const auto running = running_.load(std::memory_order_acquire);
                    std::size_t remaining = 0U;
                    {
                        std::scoped_lock lock(operation_mutex_);
                        remaining = operations_.size();
                    }
                    std::cout << "[ioring-dispatcher] observed sentinel completion streak=" << sentinel_streak
                              << " running=" << running << " operations=" << remaining << std::endl;
                }
                if (!running_.load(std::memory_order_acquire)) {
                    sentinel_streak = 0;
                    return true;
                }
                continue;
            }
            sentinel_streak = 0;
            handle_completion(completion);
        }
        return true;
    }

    void handle_completion(const IORING_CQE& completion)
    {
        std::cout << "[ioring-dispatcher] completion token=" << completion.UserData << " result=0x" << std::hex
                  << completion.ResultCode << std::dec << " bytes=" << completion.Information << std::endl;
        std::unique_ptr<Operation> operation;
        {
            std::scoped_lock lock(operation_mutex_);
            auto iter = operations_.find(completion.UserData);
            if (iter != operations_.end()) {
                operation = std::move(iter->second);
                decrement_inflight_locked(operation->file_class);
                operations_.erase(iter);
            }
        }

        if (!operation) {
            std::cout << "[ioring-dispatcher] completion missing operation token=" << completion.UserData << std::endl;
            return;
        }

        IoResult result{};
        if (FAILED(completion.ResultCode)) {
            result.status = std::error_code(static_cast<int>(HRESULT_CODE(completion.ResultCode)), std::system_category());
        } else {
            result.bytes_transferred = static_cast<std::size_t>(completion.Information);
            if (operation->is_write && any(operation->flags & IoFlag::Dsync)) {
                if (!FlushFileBuffers(operation->file)) {
                    result.status = std::error_code(static_cast<int>(GetLastError()), std::system_category());
                }
            }
        }

        if (operation->file != INVALID_HANDLE_VALUE) {
            CloseHandle(operation->file);
        }

        operation->promise.set_value(result);
        operation_cv_.notify_all();
    }

    static constexpr std::size_t class_index(FileClass file_class)
    {
        return static_cast<std::size_t>(file_class);
    }

    void decrement_inflight_locked(FileClass file_class)
    {
        const auto index = class_index(file_class);
        if (index < inflight_by_class_.size()) {
            auto& counter = inflight_by_class_[index];
            if (counter > 0U) {
                --counter;
            }
        }
    }

    AsyncIoConfig config_{};
    const std::size_t queue_capacity_;
    HIORING ring_ = nullptr;
    bool ring_closed_ = false;
    HANDLE completion_event_ = nullptr;
    std::atomic<bool> running_{false};
    std::thread completion_thread_{};
    std::atomic<ULONG_PTR> next_token_{1U};
    std::atomic<int> fatal_error_{0};

    std::mutex operation_mutex_{};
    std::condition_variable operation_cv_{};
    std::unordered_map<ULONG_PTR, std::unique_ptr<Operation>> operations_{};
    std::array<std::size_t, kFileClassCount> inflight_by_class_{};
};

#endif  // BORED_STORAGE_HAVE_IORING

std::unique_ptr<AsyncIo> create_platform_async_io(const AsyncIoConfig& config)
{
    switch (config.backend) {
        case AsyncIoBackend::ThreadPool:
            return nullptr;
        case AsyncIoBackend::WindowsIoRing:
#if BORED_STORAGE_HAVE_IORING
            return std::make_unique<IoRingDispatcher>(config);
#else
            throw std::system_error(std::make_error_code(std::errc::operation_not_supported),
                                    "Windows IORing not available in this build");
#endif
        case AsyncIoBackend::LinuxIoUring:
#if defined(__linux__)
            // TODO: Provide IoUringDispatcher implementation rooted in liburing once available.
            return nullptr;
#else
            throw std::system_error(std::make_error_code(std::errc::operation_not_supported),
                                    "Linux io_uring not available on this platform");
#endif
        case AsyncIoBackend::MacDispatch:
#if defined(__APPLE__)
            return std::make_unique<DispatchAsyncIo>(config);
#else
            throw std::system_error(std::make_error_code(std::errc::operation_not_supported),
                                    "macOS dispatch backend not available on this platform");
#endif
        case AsyncIoBackend::Auto:
        default:
            break;
    }

#if defined(__APPLE__)
    try {
        return std::make_unique<DispatchAsyncIo>(config);
    } catch (const std::system_error&) {
    }
#endif

#if BORED_STORAGE_HAVE_IORING
    try {
        return std::make_unique<IoRingDispatcher>(config);
    } catch (const std::system_error&) {
    }
#endif

#if defined(__linux__)
    // TODO: Provide IoUringDispatcher implementation rooted in liburing once available.
#endif
    return nullptr;
}

}  // namespace

std::unique_ptr<AsyncIo> create_thread_pool_async_io(const AsyncIoConfig& config)
{
    return std::make_unique<ThreadPoolAsyncIo>(config);
}

std::unique_ptr<AsyncIo> create_async_io(const AsyncIoConfig& config)
{
    if (config.backend == AsyncIoBackend::ThreadPool) {
        return create_thread_pool_async_io(config);
    }

    if (auto backend = create_platform_async_io(config)) {
        return backend;
    }

    return create_thread_pool_async_io(config);
}

}  // namespace bored::storage
