---
trigger: always_on
---

# C++20 Core Guidelines & Coding Rules

## Context
- **Standard**: C++20 (`-std=c++20`, `CXX_EXTENSIONS OFF`)
- **Build & Tooling**: CMake (target-scoped `target_*` only), `clang-format` & `clang-tidy` enforced (`-Wall -Wextra -Wpedantic -Wconversion -Werror`)
- **Sanitizers**: Debug builds must pass `ASan + UBSan` (`-fsanitize=address,undefined`) and `TSan` cleanly.

## Core Rules (C++ Core Guidelines)
1. **RAII & Rule of Zero/Five (`R.1`, `C.20`, `C.21`)**:
   - Bind all resources (memory, sockets, timers, file descriptors, locks) to object lifetime.
   - Prefer Rule of Zero. If defining any destructor/copy/move operation, explicitly define or `= delete` all five (`~T()`, copy, move). Never use raw `new`/`delete`.
2. **Ownership & Smart Pointers (`R.20`–`R.23`, `F.7`)**:
   - Use `std::unique_ptr<T>` for exclusive ownership and `std::shared_ptr<T>` only for shared async/coroutine lifetimes.
   - Raw pointers (`T*`) and references (`T&`) are strictly non-owning observers.
3. **Coroutine & Async Lifetime Safety (`CP.51`, `CP.52`)**:
   - Never capture `this` or stack references across coroutine suspension points (`co_await`) unless backed by a `std::shared_ptr` (`auto self = shared_from_this()` / `auto self = impl_`).
   - Do not hold non-coroutine locks (`std::mutex`) across `co_await` suspension points.
4. **Immutability & Compile-Time Evaluation (`Con.1`–`Con.5`)**:
   - Default variables, parameters, and methods to `const` and `constexpr`. Mark non-throwing hot-path methods `noexcept` and pure value getters `[[nodiscard]]`.
5. **Non-Owning Views & Modern Types (`SL.str.2`, `Bounds.4`)**:
   - Use `std::string_view` instead of `const std::string&` for read-only string arguments.
   - Use `std::span<const T>` / `BytesView` instead of `(const T* ptr, size_t len)` pairs.
6. **Strict Type & Cast Safety (`Enum.3`, `ES.48`, `ES.49`)**:
   - Always use scoped `enum class` with explicit underlying width (`: std::uint8_t`).
   - Never use C-style casts; use `static_cast` (or `std::bit_cast` where applicable) and avoid narrowing conversions.
   - Mark single-argument constructors `explicit` (`C.46`).
7. **Error Handling (`E.2`, `E.6`)**:
   - Use `Result<T>` (`std::expected` / `std::optional`) and marked `noexcept` on operational/hot paths for predictable failures (RPC, ABI decode, timeout).
   - Reserve exceptions (`ConfigError`, `AbiError`) strictly for startup/configuration invariants. Never return magic error codes (`-1`, `nullptr` on failure) or swallow errors silently.
8. **Header & Namespace Hygiene (`SF.2`, `SF.7`, `SF.8`)**:
   - Use `#pragma once` in all headers. Never write `using namespace ...;` at file scope in headers.
   - Keep public headers (`include/`) free of heavy internal implementation headers (use PIMPL where appropriate).
