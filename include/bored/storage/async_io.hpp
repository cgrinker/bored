#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <span>
#include <system_error>

namespace bored::storage {

enum class FileClass : std::uint8_t {
	Data,
	WriteAheadLog
};

enum class IoFlag : std::uint32_t {
	None = 0,
	Dsync = 1U << 0,
	HighPriority = 1U << 1
};

constexpr IoFlag operator|(IoFlag lhs, IoFlag rhs)
{
	return static_cast<IoFlag>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
}

constexpr IoFlag operator&(IoFlag lhs, IoFlag rhs)
{
	return static_cast<IoFlag>(static_cast<std::uint32_t>(lhs) & static_cast<std::uint32_t>(rhs));
}

constexpr bool any(IoFlag flags)
{
	return static_cast<std::uint32_t>(flags) != 0U;
}

struct IoDescriptor {
	std::filesystem::path path{};
	std::uint64_t offset = 0U;
	FileClass file_class = FileClass::Data;
};

struct ReadRequest final : IoDescriptor {
	std::byte* data = nullptr;
	std::size_t size = 0U;
};

struct WriteRequest final : IoDescriptor {
	const std::byte* data = nullptr;
	std::size_t size = 0U;
	IoFlag flags = IoFlag::None;
};

struct IoResult {
	std::size_t bytes_transferred = 0U;
	std::error_code status{};
};

enum class AsyncIoBackend : std::uint8_t {
	Auto,
	ThreadPool,
	WindowsIoRing,
	LinuxIoUring,
	MacDispatch
};

#ifndef BORED_STORAGE_PREFER_FULL_FSYNC
#    define BORED_STORAGE_PREFER_FULL_FSYNC 1
#endif

struct AsyncIoConfig {
	std::size_t worker_threads = 4U;
	std::size_t queue_depth = 128U;
	std::chrono::milliseconds shutdown_timeout{1000};
	AsyncIoBackend backend = AsyncIoBackend::Auto;
	bool use_full_fsync = BORED_STORAGE_PREFER_FULL_FSYNC != 0;
};

class AsyncIo {
public:
	virtual ~AsyncIo() = default;

	[[nodiscard]] virtual std::future<IoResult> submit_read(ReadRequest request) = 0;
	[[nodiscard]] virtual std::future<IoResult> submit_write(WriteRequest request) = 0;
	[[nodiscard]] virtual std::future<IoResult> flush(FileClass file_class) = 0;
	virtual void shutdown() = 0;
};

[[nodiscard]] std::unique_ptr<AsyncIo> create_async_io(const AsyncIoConfig& config = {});
[[nodiscard]] std::unique_ptr<AsyncIo> create_thread_pool_async_io(const AsyncIoConfig& config = {});

}  // namespace bored::storage
