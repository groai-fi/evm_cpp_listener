# C++ DeFi Stack & EVM Listener Technical Architecture

This document outlines the technical stack, C++20 dependency choices, and architectural boundaries for the **EVM C++ Log Listener** and related DeFi trading components.

---

## Part 1: What You Need to Track Events from EVM Providers

Connecting to secure WebSocket endpoints (`wss://`) and processing blockchain event logs at high throughput requires:

* **`OpenSSL`:** Provides the SSL/TLS context required by Boost.Asio for encrypted `wss://` and `https://` endpoints.
* **`Boost.Beast` + `Boost.Asio`:** Beast provides the RFC 6455 WebSocket protocol layer (handshakes, framing, ping/pong keepalives) on top of Asio's asynchronous event loop.
* **`simdjson`:** Ultra-fast JSON parsing using SIMD instructions.
  > [!IMPORTANT]
  > `simdjson` is a **parse-only** library and requires `simdjson::SIMDJSON_PADDING` (64 bytes) allocated past the buffer end. To avoid heap allocations, allocate one reusable padded buffer (`simdjson::padded_string` or a pre-allocated vector with padding) per connection. For outgoing JSON-RPC requests, use **Boost.JSON** or formatted string builders.
* **`ethash` (Keccak-256):** Used to compute event topic0 hashes locally at startup (e.g., `keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)")`) for subscription filtering and verification.

### Event Tracking Workflow:
1. Initialize `boost::asio::io_context` and `boost::asio::ssl::context`.
2. Connect `boost::beast::websocket::stream` to the primary WebSocket provider (e.g. QuickNode or Infura).
3. Send JSON-RPC subscription payloads (`eth_subscribe("logs", ...)` and `eth_subscribe("newHeads")`) built via string builders / Boost.JSON.
4. Ingest raw WebSocket frames into a reusable padded buffer and parse fields (`topics`, `data`, `blockNumber`, `logIndex`) via `simdjson::ondemand`.
5. Dispatch logs to the deduplication engine and sink them into Redis Streams via `redis-plus-plus` Lua scripts.

---

## Part 2: What You Need for Transaction Signing & Execution (Downstream Bots)

> [!NOTE]
> The **EVM Log Listener** (`evm_cpp_listener`) is strictly a **read-only ingestion pipeline** that streams logs into Redis. Transaction signing, RLP encoding, and execution are the responsibility of downstream trading/hedging bots consuming from Redis Streams.

For downstream execution bots that sign and broadcast transactions:
* **`intx` (256-bit EVM Math):** A header-only, zero-dependency C++ library for 256-bit arithmetic (`uint256`), widely used in production Ethereum clients like Silkworm. Available on ConanCenter.
* **`libsecp256k1`:** High-performance C library for ECDSA signing and public key recovery (`secp256k1_ecdsa_sign_recoverable`).
* **RLP Serialization:** Serialize dynamic EIP-1559 transactions into byte streams before signing.

---

## Part 3: EVM Simulation with `evmone` vs. Testing the Listener

### What `evmone` Is:
**`evmone`** (developed by the Ethereum Foundation's Ipsilon team) is the ultra-fast, C++ implementation of the Ethereum Virtual Machine execution engine. In an on-chain execution/arbitrage bot, `evmone` acts as a local in-memory simulator:
- It executes smart contract bytecode directly in memory using the EVMC Host interface.
- It determines if a trade is profitable in microseconds before broadcasting it to the sequencer.

```
[ Raw Event Log ] ──► [ simdjson parse ] ──► [ evmone memory simulation ] ──► [ secp256k1 sign ] ──► [ broadcast tx ]
```

### Architectural Clarification & Testing Pushback:
> [!WARNING]
> **Do not use `evmone` to test the EVM Log Listener.**
> `evmone` is a pure bytecode execution engine. It does **not** provide JSON-RPC endpoints, block headers, `eth_subscribe`, logs-by-filter, or blockchain reorgs.
>
> Testing the EVM Log Listener requires:
> 1. **Unit & Integration Tests:** An in-process, scripted mock JSON-RPC server (WebSocket & HTTP) for precise error injection (429 rate limits, disconnects, malformed frames).
> 2. **End-to-End Tests:** **Foundry Anvil** (`anvil_reorg`, `evm_mine`), which provides real JSON-RPC semantics, real block reorganizations, and real event emission.

---

## Part 4: Dependency Management & Conan 2 Configuration

Dependencies are managed using **Conan 2 (Conan 2.x)**:

### Package Availability on ConanCenter:
| Package | Available on ConanCenter | Purpose |
|---|---|---|
| `boost` (≥ 1.86.0) | ✅ Yes | Networking (`asio`, `beast`), JSON |
| `openssl` (3.x) | ✅ Yes | TLS/SSL for WebSocket and HTTP |
| `simdjson` (3.x) | ✅ Yes | SIMD-accelerated JSON parsing |
| `redis-plus-plus` | ✅ Yes | Redis Streams & Lua script pipeline |
| `intx` | ✅ Yes | 256-bit arithmetic |
| `evmc` | ✅ Yes | EVM Connector interfaces |
| `secp256k1` | ✅ Yes | Cryptographic signing (downstream) |
| `evmone` | ❌ **Not on ConanCenter** | Bytecode execution engine (downstream only, git submodule/vendored) |

### 1. `conanfile.txt` (for EVM Log Listener)
```plaintext
[requires]
boost/1.86.0
openssl/3.3.2
simdjson/3.10.1
redis-plus-plus/1.3.12

[generators]
CMakeToolchain
CMakeDeps

[layout]
cmake_layout
```

### 2. `CMakeLists.txt`
```cmake
cmake_minimum_required(VERSION 3.20)
project(evm_listener LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Conan Packages
# Note: Boost >= 1.89 removed the standalone Boost.System stub library; Boost::headers is used.
find_package(Boost REQUIRED COMPONENTS headers)
find_package(OpenSSL REQUIRED)
find_package(simdjson REQUIRED)
find_package(redis-plus-plus REQUIRED)

add_executable(evm_listener
    src/main.cpp
)

target_link_libraries(evm_listener
    PRIVATE
        Boost::headers
        OpenSSL::SSL
        OpenSSL::Crypto
        simdjson::simdjson
        redis-plus-plus::redis-plus-plus
)

# Compiler Optimizations:
# Use -O2 / -O3 and optional LTO (-flto).
# Do NOT use -march=native (breaks portability across ARM64 containers and x86_64 prod).
# Do NOT use -ffast-math (breaks IEEE-754 precision compliance).
target_compile_options(evm_listener PRIVATE
    $<$<CONFIG:Release>:-O3 -flto>
)
```

### 3. Build Commands with Conan 2
```bash
# 1. Install dependencies using Conan 2
conan install conanfile.txt --build=missing -s build_type=Release

# 2. Configure CMake using the Conan preset
cmake --preset conan-release

# 3. Compile the binary
cmake --build --preset conan-release
```
