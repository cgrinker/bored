# bored

Cross-platform C++ starter project configured for [vcpkg](https://github.com/microsoft/vcpkg) manifest mode and CMake. The sample greeter library exposes a simple `bored::greeting()` helper, along with a Catch2-powered test suite.

## Prerequisites

- CMake 3.21+
- Microsoft Visual Studio 2022 Build Tools (or another C++20-capable compiler)
- vcpkg cloned locally with `VCPKG_ROOT` set (manifest mode is used automatically)

## Configure

```powershell
cmake -S . -B build "-DCMAKE_TOOLCHAIN_FILE=$Env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" -DVCPKG_TARGET_TRIPLET=x64-windows
```

## Build

```powershell
cmake --build build --config Release
```

## Test

```powershell
ctest --test-dir build --config Release
```

## Run

```powershell
./build/Release/bored.exe
```

## Customization

- Update `vcpkg.json` to add new dependencies. Run the configure step again to fetch them.
- Add new headers under `include/` and implementation files under `src/`. Register additional sources in `CMakeLists.txt` as needed.
- Tests live in `tests/` and are discovered automatically via `catch_discover_tests`.
- Storage-layer design notes live in `docs/storage.md`; expand this document as the database grows.
- CRC32C helpers for pages and WAL records live in `include/bored/storage/checksum.hpp` and are exercised in `tests/storage_format_tests.cpp`.
- Tuple-level WAL payload helpers are in `include/bored/storage/wal_payloads.hpp` for constructing insert/update/delete records.
- Free space tracking and page compaction utilities reside in `include/bored/storage/free_space_map.hpp` and `include/bored/storage/page_operations.hpp` (`compact_page`), with coverage in `tests/storage_format_tests.cpp`.
- Cross-platform asynchronous I/O scaffolding lives in `include/bored/storage/async_io.hpp`; call `create_async_io()` to obtain the best available backend (configure via `AsyncIoBackend` when you want to force the thread pool or the Windows/Linux ring variants). The portable fallback lives in `src/storage/async_io.cpp` with coverage in `tests/async_io_tests.cpp`.
