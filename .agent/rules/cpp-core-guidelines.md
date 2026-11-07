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
   - Scope C++20 coroutines (`co_await`) strictly to **cold/async I/O paths** (RPC clients, WebSocket connection loops allocated once at startup). Never spawn coroutine frames or touch `std::shared_ptr` ref-counts per tick on the hot path.
   - Never capture `this` or stack references across coroutine suspension points (`co_await`) unless backed by a `std::shared_ptr` (`auto self = shared_from_this()` / `auto self = impl_`).
   - Do not hold non-coroutine locks (`std::mutex`) across `co_await` suspension points.
4. **Immutability & Compile-Time Evaluation (`Con.1`–`Con.5`)**:
   - Default variables, parameters, and methods to `const` and `constexpr`. Mark non-throwing hot-path methods `noexcept` and pure value getters `[[nodiscard]]`.
5. **Non-Owning Views & Modern Types (`SL.str.2`, `Bounds.4`)**:
   - Use `std::string_view` instead of `const std::string&` for read-only string arguments.
   - Use `std::span<const T>` / `BytesView` instead of `(const T* ptr, size_t len)` pairs.
6. **Strict Type, Cast & Numerical Determinism (`Enum.3`, `ES.48`, `ES.49`)**:
   - Always use scoped `enum class` with explicit underlying width (`: std::uint8_t`).
   - Never use C-style casts; use `static_cast` (or `std::bit_cast` where applicable) and avoid narrowing conversions. Mark single-argument constructors `explicit` (`C.46`).
   - **Financial & Replay Determinism**: Never use floating-point (`float`/`double`) for token balances, wei/wad/ray amounts, or pool ticks—use exact fixed-point integers (`int32_t`, `uint64_t`, `uint128_t`, `uint256_t`). For quantitative floating-point math (Kalman, Monte Carlo), forbid `-ffast-math` and enforce `-ffp-contract=off` for bit-exact cross-platform replay determinism.
7. **Error Handling (`E.2`, `E.6`)**:
   - Use a trivially copyable `Result<T, ErrorCode>` (`std::optional<T>` or a custom value-type `Result` backed by `enum class ErrorCode : std::uint8_t`, fitting in `<= 16` bytes for register return) marked `noexcept` on operational/hot paths. Do not use heap-allocating error strings on the hot path.
   - Reserve exceptions (`ConfigError`, `AbiError`) strictly for cold startup/configuration invariants. Never return magic error codes (`-1`, `nullptr` on failure) or swallow errors silently.
8. **Header & Namespace Hygiene (`SF.2`, `SF.7`, `SF.8`)**:
   - Use `#pragma once` in all headers. Never write `using namespace ...;` at file scope in headers.
   - Use **scoped namespace aliases** (`namespace active_arch = arch::avx512;`) or type aliases (`using ActiveKernel = ...;`) inside module namespaces for compile-time backend selection.
   - Keep public headers (`include/`) free of heavy internal I/O headers; restrict `PIMPL` strictly to **cold-path / async I/O classes** (never on hot-path DOD structs or templates, which must remain header-visible for inlining).
9. **Data-Oriented Design (DOD) & Cache Locality on Hot Paths (`Per.7`, `Per.19`)**:
   - **Hot vs. Cold Boundary**: Apply DOD strictly to latency-critical paths (market ingestion, Kalman/microstructure brain, `/dev/shm` IPC structs, and SIMD Monte Carlo solvers). Keep cold startup and async RPC I/O in standard RAII C++20.
   - **Contiguous Memory & `SoA`**: Prefer contiguous flat storage (`std::array`, pre-allocated `std::vector`, power-of-two ring buffers) over pointer-chasing node containers (`std::map`, `std::unordered_map`, `std::list`). Use Structure-of-Arrays (`SoA`) for SIMD/AVX-512 batch loops so every vector load is unit-stride.
   - **Cache-Line Alignment & IPC Invariants**: Align shared-memory (`Seqlock`, SPSC queues) and DMA structs to 64-byte cache lines (`alignas(64)`) to prevent false sharing. Enforce binary layout invariants at compile time with `static_assert(std::is_trivially_copyable_v<T>)`, `static_assert(std::is_standard_layout_v<T>)`, and exact `sizeof(T)` checks. For atomic sequence counters inside `trivially_copyable` `/dev/shm` structs, store raw aligned integers (`alignas(64) std::uint64_t`) and access them via C++20 `std::atomic_ref<std::uint64_t>`.
10. **Zero Allocation, Static Dispatch & Branchless Hot Loops (`Per.11`, `C.120`)**:
    - **Zero Hot-Path Heap Allocation**: Never allocate or resize heap memory (`new`, `malloc`, `std::vector::push_back` without capacity, `std::string`, heap-boxed `std::function`, or `std::shared_ptr` ref-count churn) inside the tick processing loop. Pre-allocate all working buffers at startup.
    - **Static Polymorphism over `virtual`**: Do not use `virtual` methods or dynamic dispatch (`dynamic_cast`) on the hot path; use C++20 `concepts`, templates, stateless policy `struct`s, or `std::variant` to enable inlining.
    - **Branchless Hot-Path Math**: Prefer branchless expressions (`std::clamp`, conditional moves, arithmetic masks) in tight numerical and SIMD loops to avoid CPU branch mispredictions.
