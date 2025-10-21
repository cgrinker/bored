#include "bored/storage/async_io.hpp"

#include <condition_variable>
#include <deque>
#include <fstream>
#include <future>
#include <mutex>
#include <system_error>
#include <thread>
#include <vector>

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

std::unique_ptr<AsyncIo> create_platform_async_io(const AsyncIoConfig& config)
{
    (void)config;

#if defined(_WIN32)
    // TODO: Provide IoRingDispatcher implementation that bridges to Windows IORing APIs.
    return nullptr;
#elif defined(__linux__)
    // TODO: Provide IoUringDispatcher implementation rooted in liburing once available.
    return nullptr;
#else
    return nullptr;
#endif
}

}  // namespace

std::unique_ptr<AsyncIo> create_thread_pool_async_io(const AsyncIoConfig& config)
{
    return std::make_unique<ThreadPoolAsyncIo>(config);
}

std::unique_ptr<AsyncIo> create_async_io(const AsyncIoConfig& config)
{
    if (auto backend = create_platform_async_io(config)) {
        return backend;
    }

    return create_thread_pool_async_io(config);
}

}  // namespace bored::storage
