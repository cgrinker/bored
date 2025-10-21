#include "bored/storage/async_io.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <future>
#include <mutex>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>
#include <string>
#include <limits>
#include <type_traits>

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

#if BORED_STORAGE_HAVE_IORING

class IoRingDispatcher final : public AsyncIo {
public:
    explicit IoRingDispatcher(const AsyncIoConfig& config)
        : config_{config}
    {
        const ULONG queue_depth = static_cast<ULONG>(std::max<std::size_t>(1U, config_.queue_depth));
        IORING_CREATE_FLAGS flags{};
        const auto hr = CreateIoRing(IORING_VERSION_3, flags, queue_depth, queue_depth, &ring_);
        if (FAILED(hr)) {
            throw std::system_error(static_cast<int>(HRESULT_CODE(hr)), std::system_category(), "CreateIoRing failed");
        }

        running_.store(true, std::memory_order_release);
        completion_thread_ = std::thread([this]() { completion_loop(); });
    }

    ~IoRingDispatcher() override
    {
        shutdown();
        if (ring_ != nullptr) {
            CloseIoRing(ring_);
            ring_ = nullptr;
        }
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
            (void)file_class;
            for (;;) {
                {
                    std::scoped_lock lock(operation_mutex_);
                    if (operations_.empty()) {
                        break;
                    }
                }
                SubmitIoRing(ring_, 0, 1, nullptr);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            IoResult result{};
            result.status.clear();
            return result;
        });
    }

    void shutdown() override
    {
        bool expected = true;
        if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            return;
        }

        SubmitIoRing(ring_, 0, 0, nullptr);
        if (completion_thread_.joinable()) {
            completion_thread_.join();
        }

        std::unordered_map<ULONG_PTR, std::unique_ptr<Operation>> remaining;
        {
            std::scoped_lock lock(operation_mutex_);
            remaining.swap(operations_);
        }

        for (auto& [key, operation] : remaining) {
            (void)key;
            if (operation->file != INVALID_HANDLE_VALUE) {
                CancelIoEx(operation->file, nullptr);
                CloseHandle(operation->file);
            }
            IoResult result{};
            result.status = std::make_error_code(std::errc::operation_canceled);
            operation->promise.set_value(result);
        }
    }

private:
    struct Operation {
        std::promise<IoResult> promise{};
        HANDLE file = INVALID_HANDLE_VALUE;
        std::filesystem::path path{};
        std::uint64_t offset = 0U;
        FileClass file_class = FileClass::Data;
        IoFlag flags = IoFlag::None;
        std::byte* read_buffer = nullptr;
        const std::byte* write_buffer = nullptr;
        std::size_t size = 0U;
        bool is_write = false;
        ULONG_PTR token = 0U;
    };

    template <typename Request>
    std::future<IoResult> submit_operation(Request request)
    {
        static_assert(std::is_same_v<Request, ReadRequest> || std::is_same_v<Request, WriteRequest>, "Unsupported request type");

        if constexpr (std::is_same_v<Request, ReadRequest>) {
            if (request.data == nullptr || request.size == 0U) {
                std::promise<IoResult> promise;
                auto future = promise.get_future();
                promise.set_value(IoResult{0U, std::make_error_code(std::errc::invalid_argument)});
                return future;
            }
        } else {
            if (request.data == nullptr || request.size == 0U) {
                std::promise<IoResult> promise;
                auto future = promise.get_future();
                promise.set_value(IoResult{0U, std::make_error_code(std::errc::invalid_argument)});
                return future;
            }
        }

        const auto handle = open_file(request);
        if (handle == INVALID_HANDLE_VALUE) {
            const auto error = GetLastError();
            std::promise<IoResult> promise;
            auto future = promise.get_future();
            promise.set_value(IoResult{0U, std::error_code(static_cast<int>(error), std::system_category())});
            return future;
        }

        auto operation = std::make_unique<Operation>();
        operation->file = handle;
        operation->path = request.path;
        operation->offset = request.offset;
        operation->file_class = request.file_class;
        operation->token = reinterpret_cast<ULONG_PTR>(operation.get());

        if constexpr (std::is_same_v<Request, ReadRequest>) {
            operation->is_write = false;
            operation->read_buffer = request.data;
            operation->size = request.size;
            operation->flags = IoFlag::None;
        } else {
            operation->is_write = true;
            operation->write_buffer = request.data;
            operation->size = request.size;
            operation->flags = request.flags;
        }

        auto future = operation->promise.get_future();
        const auto token = operation->token;

        {
            std::scoped_lock lock(operation_mutex_);
            operations_.emplace(token, std::move(operation));
        }

        HRESULT hr = submit_to_ring(token);
        if (FAILED(hr)) {
            std::unique_ptr<Operation> cleanup;
            {
                std::scoped_lock lock(operation_mutex_);
                auto iter = operations_.find(token);
                if (iter != operations_.end()) {
                    cleanup = std::move(iter->second);
                    operations_.erase(iter);
                }
            }
            if (cleanup) {
                if (cleanup->file != INVALID_HANDLE_VALUE) {
                    CloseHandle(cleanup->file);
                }
                IoResult result{};
                result.status = std::error_code(static_cast<int>(HRESULT_CODE(hr)), std::system_category());
                cleanup->promise.set_value(result);
            }
            return future;
        }

        SubmitIoRing(ring_, 0, 0, nullptr);
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
        std::scoped_lock lock(operation_mutex_);
        auto iter = operations_.find(token);
        if (iter == operations_.end()) {
            return E_FAIL;
        }

        auto* operation = iter->second.get();
        const auto handle_ref = IoRingHandleRefFromHandle(operation->file);
        if (operation->size > std::numeric_limits<ULONG>::max()) {
            return HRESULT_FROM_WIN32(ERROR_INVALID_PARAMETER);
        }

        const auto length = static_cast<ULONG>(operation->size);
        if (operation->is_write) {
            auto buffer_ref = IoRingBufferRefFromPointer(const_cast<std::byte*>(operation->write_buffer));
            const auto write_flags = any(operation->flags & IoFlag::Dsync) ? FILE_WRITE_FLAGS_WRITE_THROUGH : FILE_WRITE_FLAGS_NONE;
            return BuildIoRingWriteFile(ring_, handle_ref, buffer_ref, length, operation->offset, write_flags, operation->token, IOSQE_FLAGS_NONE);
        }

        auto buffer_ref = IoRingBufferRefFromPointer(operation->read_buffer);
        return BuildIoRingReadFile(ring_, handle_ref, buffer_ref, length, operation->offset, operation->token, IOSQE_FLAGS_NONE);
    }

    void completion_loop()
    {
        while (running_.load(std::memory_order_acquire)) {
            SubmitIoRing(ring_, 0, 1, nullptr);
            drain_completions();
        }

        SubmitIoRing(ring_, 0, 0, nullptr);
        drain_completions();
    }

    void drain_completions()
    {
        IORING_CQE completion{};
        while (SUCCEEDED(PopIoRingCompletion(ring_, &completion))) {
            handle_completion(completion);
        }
    }

    void handle_completion(const IORING_CQE& completion)
    {
        std::unique_ptr<Operation> operation;
        {
            std::scoped_lock lock(operation_mutex_);
            auto iter = operations_.find(completion.UserData);
            if (iter != operations_.end()) {
                operation = std::move(iter->second);
                operations_.erase(iter);
            }
        }

        if (!operation) {
            return;
        }

        IoResult result{};
        if (FAILED(completion.ResultCode)) {
            result.status = std::error_code(static_cast<int>(HRESULT_CODE(completion.ResultCode)), std::system_category());
        } else {
            result.bytes_transferred = static_cast<std::size_t>(completion.Information);
            if (operation->is_write && any(operation->flags & IoFlag::Dsync)) {
                FlushFileBuffers(operation->file);
            }
        }

        if (operation->file != INVALID_HANDLE_VALUE) {
            CloseHandle(operation->file);
        }

        operation->promise.set_value(result);
    }

    AsyncIoConfig config_{};
    HIORING ring_ = nullptr;
    std::atomic<bool> running_{false};
    std::thread completion_thread_{};

    std::mutex operation_mutex_{};
    std::unordered_map<ULONG_PTR, std::unique_ptr<Operation>> operations_{};
};

#endif  // BORED_STORAGE_HAVE_IORING

std::unique_ptr<AsyncIo> create_platform_async_io(const AsyncIoConfig& config)
{
    switch (config.backend) {
        case AsyncIoBackend::ThreadPool:
            return nullptr;
        case AsyncIoBackend::WindowsIoRing:
        case AsyncIoBackend::Auto:
#if BORED_STORAGE_HAVE_IORING
            try {
                return std::make_unique<IoRingDispatcher>(config);
            } catch (const std::system_error&) {
                if (config.backend == AsyncIoBackend::WindowsIoRing) {
                    throw;
                }
                return nullptr;
            }
#else
            if (config.backend == AsyncIoBackend::WindowsIoRing) {
                throw std::system_error(std::make_error_code(std::errc::operation_not_supported),
                                        "Windows IORing not available in this build");
            }
            return nullptr;
#endif
        case AsyncIoBackend::LinuxIoUring:
        default:
#if defined(__linux__)
            // TODO: Provide IoUringDispatcher implementation rooted in liburing once available.
            return nullptr;
#else
            if (config.backend == AsyncIoBackend::LinuxIoUring) {
                throw std::system_error(std::make_error_code(std::errc::operation_not_supported),
                                        "Linux io_uring not available on this platform");
            }
            return nullptr;
#endif
    }
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
