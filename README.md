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
