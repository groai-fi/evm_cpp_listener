# FPGA vs. CPU/GPU Architecture for DeFi, Real-Time ML/RL, and Hardware Use Cases

> **Bottom Line Up Front (BLUF)**:
> 1. **Do Not Use FPGAs for Public Internet `wss://` Ingestion:** Neither [`evm_cpp_listener`](../System_Modules_and_Architecture.md) nor `market-data-streamer` should be moved onto an FPGA to parse public internet WebSockets. Upstream CEX and EVM feeds stream encrypted **TLS 1.3 over stateful TCP + variable-length ASCII JSON** across oceans ($15\text{–}180\text{ ms}$ transit) with Base L2 block intervals of **$250\text{–}2,000\text{ ms}$**. On a $5.0\text{ GHz}$ server CPU, `Boost.Asio` + `simdjson` + `ONNX Runtime` / `Treelite` already processes ticks and evaluates ML/RL policies in **$10\text{–}25\ \mu\text{s}$** inside L1/L2 cache with zero PCIe/AXI bus overhead.
> 2. **Where a Physical FPGA Board *Does* Add Real Value:** Instead of replacing the C++20 ingestion engine, attach the FPGA downstream as a **specialized hardware co-processor** for:
>    - **(A) Parallel Fixed-Point Monte Carlo LP Tick Range Simulation** (see companion spec: [`02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md`](02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md)),
>    - **(B) Local "CME/Nasdaq-Style" Binary UDP Tick-to-Trade + `hls4ml` Hardware Lab** fed by a 128-byte non-blocking UDP tap in `RedisSink::emit_fast()`,
>    - **(C) Silicon-Enforced DeFi Pre-Trade Risk Firewall & `secp256k1` Transaction Signer**, and
>    - **(D) EVM `Keccak-256` `CREATE2` Smart Contract / Uniswap v4 Hook Address Mining**.

---

## 1. Architectural Warning: Why FPGAs Do Not Fit Internet `wss://` Ingestion

> [!WARNING]
> ### Nanosecond Hardware vs. Millisecond Blockchain & Internet Physics
> - **The Conflict:** FPGAs dominate co-located TradFi equities/futures exchanges (Nasdaq Carteret `NY11`, CME Aurora) because those exchanges push **unencrypted, fixed-offset binary UDP multicast packets** (`ITCH 5.0` / `MDP 3.0 SBE`) over a $10\text{m}$ fiber cable inside the same building where the entire tick-to-trade race is **$50\text{ to } 800\text{ nanoseconds}$ ($0.00005\text{–}0.0008\text{ ms}$)**.
> - **EVM & Crypto CEX Constraints:**
>   1. **Block Time & Internet Geography:** Base L2 (`chain_id=8453`) produces blocks every **$2,000\text{ ms}$** ($250\text{ ms}$ flashblocks), and WebSocket propagation from RPC/CEX servers across Tokyo, Virginia, and Singapore takes **$15\text{ to } 180\text{ ms}$** ($15,000\text{–}180,000\ \mu\text{s}$). Saving $500\text{ ns}$ on an FPGA optimizes **$0.0006\%$** of total latency.
>   2. **Protocol Stack Mismatch (TLS 1.3 + TCP + JSON):** Every venue in `markets.toml` (`Binance`, `Kraken`, `Coinbase`, `Tiingo FX`, `Pyth Hermes`) and `base.toml` (`Alchemy`, `QuickNode`, `Infura`, `Ankr`) requires stateful TCP congestion control, TLS 1.3 ECDHE/AES-GCM decryption, WebSocket framing, and variable-length ASCII JSON parsing. C++20 `simdjson` on a $5.0\text{ GHz}$ CPU parses JSON in **$\sim 0.3\ \mu\text{s}$**, whereas a $250\text{ MHz}$ FPGA state machine cannot natively terminate TLS without a CPU.
>   3. **Multi-Hour Hardware Re-Compilation:** Synthesizing and place-and-routing an FPGA bitstream takes **2 to 6 hours** per feature change, whereas reloading an `.onnx` model in C++ takes milliseconds.

### Latency Comparison Across Compute Tiers

| Compute Tier | Typical Latency | Best Use Case | Verdict for `evm_cpp_listener` & `market-data-streamer` |
| :--- | :--- | :--- | :--- |
| **FPGA** (Verilog / HLS on Alveo / Zynq) | $0.05\text{–}0.8\ \mu\text{s}$ (wire) / $3.75\text{ ms}$ (100k MC paths) | Raw UDP `ITCH`/`OUCH` parsing, or parallel fixed-point Monte Carlo & `Keccak-256` | **Use as Downstream Co-Processor** (Monte Carlo LP simulator, UDP lab, or hardware risk signer; never for `wss://` ingestion) |
| **C++20 CPU SIMD + `Treelite` / `ONNX Runtime`** | **$2\text{–}50\ \mu\text{s}$** ($0.002\text{–}0.05\text{ ms}$) | Live feature accumulation + GBDT (`LightGBM`/`XGBoost`) or RL Actor MLP inference | **#1 Sweet Spot for Hot-Path Ingestion & Live Inference** |
| **GPU (`NVIDIA TensorRT` / `PyTorch` / `JAX`)** | $200\ \mu\text{s}\text{–}2\text{ ms}$ | Offline ML/RL training over 100M+ Parquet rows or large Transformer inference | **#1 Sweet Spot for Offline ML & RL Training** |
| **ClickHouse Incremental Materialized Views** | $5\text{–}20\text{ ms}$ | Multi-pool SQL rollups, OHLCV bars, and cross-desk dashboards (`cg_clickhouse_hot`) | **#1 Sweet Spot for Multi-Pool OLAP Aggregation** |

---

## 2. The Right Tools for Real-Time Aggregation & ML/RL Inference

### 2.1 Real-Time Feature Aggregation
1. **Hot-Path Aggregation ($\sim 1\text{–}10\ \mu\text{s}$): C++20 $O(1)$ Incremental Accumulators & Ring Buffers**
   - **Exponential Moving Average (EMA) & Volatility:** Update in $O(1)$ time per `Swap` log using only the previous state and new tick:
     $$\mu_t = \alpha \cdot p_t + (1 - \alpha) \cdot \mu_{t-1}$$
     *(Engineering translation: If Aerodrome EURC/USDC price $p_t = 1.0850$, previous average $\mu_{t-1} = 1.0840$, and decay weight $\alpha = 0.1$, the new average updates to $1.0841$ in **3 CPU clock cycles** without touching disk or a database.)*
   - **Sliding Window Order-Flow Imbalance (Fixed Ring Buffer):** Pre-allocated `std::array` or `boost::circular_buffer` maintaining 1m/5m net buy vs. sell volume in $O(1)$.
   - **Regime Shift / Trend Change Detection (Page-Hinkley / CUSUM):** Tracks cumulative price drift in $O(1)$ per event to trigger vault rebalances or circuit breakers.
2. **Multi-Pool Stream Aggregation ($\sim 5\text{–}20\text{ ms}$): ClickHouse `AggregatingMergeTree` or Rust Stream Engines (`Bytewax` / `Arroyo`)**
   - ClickHouse Live Materialized Views (Tier 2 in [`Two_Tier_Parquet_DuckDB_ClickHouse_Architecture.md`](../data_lake/Two_Tier_Parquet_DuckDB_ClickHouse_Architecture.md)) incrementally update pre-aggregated state tables in RAM using AVX-512 vectorized CPU instructions.

### 2.2 Decoupling Offline ML/RL Training from Compiled Live Inference
Never run Python `scikit-learn` or `PyTorch` eager mode directly in the hot execution loop. Decouple **Offline Training** from **Compiled Live Inference**:

```
┌────────────────────────────────────────────────────────────────────────────┐
│ 1. OFFLINE TRAINING (Tier 1 Data Lake — Python / GPU)                      │
│    Parquet Files ──► DuckDB (SQL Feature Eng) ──► Zero-Copy Arrow          │
│                  ──► LightGBM / XGBoost / PyTorch RL (PPO / SAC)           │
│                  ──► Export to .onnx or Treelite .so shared library        │
└──────────────────────────────────────┬─────────────────────────────────────┘
                                       │ Atomic model reload (0 downtime)
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ 2. REAL-TIME INFERENCE (Hot Path — C++20 Consumer)                         │
│    Redis `evml:{shard}:fast` ──► O(1) Ring Buffer Features (~1 µs)         │
│                              ──► ONNX Runtime / Treelite C++ (~5–30 µs)    │
│                              ──► Trade / Rebalance Signal                  │
└────────────────────────────────────────────────────────────────────────────┘
```

- **Gradient Boosted Decision Trees (`LightGBM` / `XGBoost` / `CatBoost`) $\rightarrow$ `Treelite` or `ONNX Runtime`:** Compiles decision trees into branchless C code with native SIMD instructions (`2–15 µs` per prediction on 1 CPU core).
- **Reinforcement Learning (PPO / SAC Actor Policy $\pi_\theta(a_t \mid s_t)$) $\rightarrow$ `ONNX Runtime` (C++ AVX-512 / Intel AMX):** Even when RL training takes millions of GPU episodes, the deployed RL Actor network is typically a compact 3-to-4 layer MLP ($128 \rightarrow 256 \rightarrow 256 \rightarrow 64$) that fits inside CPU L1/L2 cache and executes in **$10\text{–}25\ \mu\text{s}$** in full `FP32` precision.

### 2.3 Why AMD `Vitis AI` / `hls4ml` Cannot Train & Suffers from the "PCIe / AXI Tax"
1. **Strictly Inference-Only (`INT8` / `INT4` Quantized):** AMD's `Vitis AI` DPU has no backward-pass autograd hardware and lacks GPU HBM3 bandwidth ($3,350\text{ GB/s}$ on H100 vs. $\sim 77\text{ GB/s}$ DDR4 on Alveo). It requires quantizing `FP32` weights down to `INT8`/`INT4`, which can round subtle basis-point spread signals into the same integer bucket.
2. **The PCIe Round-Trip Tax ($\sim 6\text{–}14\ \mu\text{s}$):** When network packets arrive on the host CPU first, copying the state vector across PCIe DMA to an FPGA card and back takes $\sim 11\text{–}29\ \mu\text{s}$ total—slower than evaluating the model directly inside the host CPU's L1 cache ($\sim 8\text{–}25\ \mu\text{s}$).
3. **The Zynq / Versal `PetaLinux` Embedded ARM Bottleneck:**
   On Zynq UltraScale+ / Versal SoCs, pure FPGA fabric does not speak WebSockets; `PetaLinux` runs the TCP/TLS/WebSocket stack on **2 to 4 weak embedded ARM Cortex-A53/A72 cores at $1.2\text{–}1.5\text{ GHz}$** and copies payloads over `AXI-DMA` to the FPGA:

```
========================================================================================
PATH A: Current C++20 Repo on a Modern Server CPU (AMD EPYC / Ryzen @ 5.0 GHz)
========================================================================================
[NIC] ──► [5.0 GHz CPU Core: Linux TCP + TLS 1.3 + Boost.Beast WS]  (~8–12 µs)
      ──► [Same Core L1 Cache: simdjson + CompositeIndexPipeline]    (~1.5 µs)
      ──► [Same Core L1 Cache: ONNX Runtime / Treelite ML Inference] (~8–10 µs)
      ─────────────────────────────────────────────────────────────────────────
      TOTAL LATENCY: ~18–24 µs (Zero bus copies; 100% inside 5.0 GHz L1/L2 cache)

========================================================================================
PATH B: AMD Zynq / Versal SoC Running WebSockets on PetaLinux
========================================================================================
[NIC] ──► [1.3 GHz Embedded ARM Cortex-A53: PetaLinux TCP + TLS 1.3 + WS] (~45–90 µs) ◄── 5x SLOWER CPU!
      ──► [1.3 GHz Embedded ARM: Parse JSON strings into numbers]         (~8–15 µs)
      ──► [AXI DMA Bus Transfer: Copy from ARM RAM into FPGA Fabric]      (~2–5 µs)   ◄── Extra DMA copy!
      ──► [FPGA Fabric DPU: Run INT8 Quantized Model]                     (~5–10 µs)
      ──► [AXI DMA Bus Transfer: Copy prediction back to ARM to send TX]  (~2–5 µs)
      ─────────────────────────────────────────────────────────────────────────
      TOTAL LATENCY: ~62–125 µs (3x to 5x SLOWER than Path A!)
```

- **Why `PetaLinux` Chokes on High Connection Counts:** Zynq/Versal + `PetaLinux` was designed for smart cameras/radars where raw sensor pixels enter FPGA pins directly and only 1–2 human operators connect to `PetaLinux` over a WebSocket dashboard. If you send thousands of network connections per second into `PetaLinux`, its 2–4 weak $1.5\text{ GHz}$ ARM cores hit 100% CPU saturation on network interrupts while the FPGA sits 95% idle.
- **Actual DeFi Inference Rate:** `market-data-streamer` (`min_emit_interval_ms = 25` $\rightarrow$ $80\text{ ticks/s}$) + `evm_cpp_listener` (Base L2 $\rightarrow$ $5\text{–}50\text{ swaps/s}$) totals **$\sim 150\text{ inferences/sec}$**, which consumes **$0.22\%$ of a single server CPU core** ($150 \times 15\ \mu\text{s} = 2,250\ \mu\text{s/s}$).

---

## 3. Cloud Hardware & TradFi-to-Base-L2 RWA Arbitrage

### 3.1 AWS Instance Comparison (`c7i`/`c7a` vs. `g6` vs. `inf2` vs. `f1`/`f2` FPGA)
On AWS EC2 `f1` and `f2` FPGA instances, Amazon physically disconnects the FPGA from the VPC network card (`AWS Nitro ENA`) for security—every packet is forced through `Nitro ENA -> Host x86 CPU -> PCIe XDMA -> FPGA -> PCIe XDMA -> Host CPU`.

| AWS Instance Family | Hardware Inside | On-Demand Price | Single-Event Inference Latency | Max Throughput | Verdict for This Stack |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`c7a.2xlarge` / `c7i.2xlarge`** (Compute CPU) | 8 vCPU **AMD EPYC Genoa (`AVX-512`)** or **Intel Sapphire Rapids (`AMX` matrix engine)** | **$\$0.35\text{–}\$0.40/\text{hr}$** ($\sim \$260/\text{mo}$) | **$5\text{–}25\ \mu\text{s}$** (Zero PCIe copy; runs in L1 cache!) | $100,000+\text{ req/s}$ | **#1 Choice for Live 24/7 Bot + Listeners** (deploy in `us-east-1` N. Virginia next to Base Sequencer & Coinbase) |
| **`g6.xlarge`** (NVIDIA GPU) | **NVIDIA L4 Tensor Core GPU** ($24\text{ GB}$ VRAM, $242\text{ TFLOPS}$ FP16) | **$\$0.80/\text{hr}$** (or **$\$0.25/\text{hr}$ Spot**) | $150\text{–}800\ \mu\text{s}$ | $500,000+\text{ req/s}$ | **#1 Choice for Offline ML / RL Training** on Tier 1 Parquet files |
| **`inf2.xlarge`** (AWS Inferentia2) | Custom AWS **NeuronCore-v2** ASIC ($190\text{ TFLOPS}$ FP16/BF16) | **$\$0.76/\text{hr}$** ($\sim \$550/\text{mo}$) | $100\text{–}500\ \mu\text{s}$ | $300,000+\text{ req/s}$ | Built for high-volume LLM APIs; unnecessary for 150 ticks/sec |
| **`f1.2xlarge` / `f2.6xlarge`** (AWS FPGA) | Xilinx Virtex UltraScale+ VU9P / Versal over PCIe (No NIC port) | **$\$1.65\text{–}\$1.98/\text{hr}$** ($\sim \$1,300/\text{mo}$) | $20\text{–}40\ \mu\text{s}$ (hurt by mandatory PCIe DMA) | Limited by PCIe & `INT8` | **Avoid on AWS:** $5\times$ pricier than `c7i` and has no direct network connection |

### 3.2 TradFi Co-Location vs. Non-Colocated Base L2 RWA Arbitrage
- **Why Direct Nasdaq/CME FPGA Requires Co-Location:** Running an AMD Alveo `UL3524` FPGA on raw Nasdaq (`ITCH 5.0` / `OUCH 5.0`) or CME (`MDP 3.0` / `iLink 3`) achieves $\sim 110\text{–}250\text{ ns}$ tick-to-trade, but requires a physical rack inside **Equinix `NY11` (Carteret, NJ)** or **CME Aurora (IL)** ($\sim \$20\text{k}\text{–}\$50\text{k}/\text{mo}$) plus SEC Rule 15c3-5 DMA clearing sponsorship.
- **How to Get Real-Time TradFi Data Without Co-Location for Base L2 RWA Arbitrage:**
  1. **Pyth Hermes (`hermes.pyth.network` — $0 Free, already in `markets.toml`):** Co-located HFTs (Jump, Jane Street, Virtu, Cboe) publish live $100\text{–}400\text{ ms}$ US Equities/ETFs (`SPY`, `QQQ`, `NVDA`, `TLT`), FX (`EUR/USD`), and Metals (`XAU/USD` Gold) directly to Pyth SSE.
  2. **Tiingo IEX (`wss://api.tiingo.com/iex` — $0–$30/mo, key already in `markets.toml`) / Alpaca IEX:** The IEX Exchange charges $0 exchange fees for real-time US stock/ETF top-of-book quotes (`SPY`, `QQQ`, `TLT`, `GLD`).
  3. **Databento (`databento.com` — Pay-as-you-go):** Streams raw CME Globex (`ES`, `NQ`, `GC`, `ZN`, `6E`) and Nasdaq `ITCH` over internet TCP to C++ without renting a co-location rack.
- **Base L2 RWA Liquidity Tiers:**
  - **Tier 1 (High Liquidity, Trade Now):** Fiat FX Stablecoins (**`EURC/USDC`** on Aerodrome Slipstream, $\$5\text{M}\text{–}\$20\text{M}+$ TVL—already live in both repos).
  - **Tier 2 (Medium Liquidity):** Tokenized Gold (`PAXG`/`XAUT`) and RWA Perp DEXs (`Ostium`, `Synthetix`).
  - **Tier 3 (Yield Carry / Depeg Monitoring):** Treasury RWAs (`Ondo USDY`, `USDM`, `sUSDS`).
  - **Tier 4 (Low DEX TVL / KYC Gated — Caution):** Individual tokenized stocks (`Backed bNVDA`/`bCSPX`) currently have thin on-chain DEX depth ($\$10\text{k}\text{–}\$200\text{k}$).

---

## 4. Four High-Value Use Cases for Your Physical FPGA Board

While C++20 handles `wss://` ingestion on the host CPU, your physical FPGA board can be wired directly into this stack for four high-value applications:

### Use Case 1: Parallel Fixed-Point Monte Carlo Simulator for LP Tick Ranges
- **Detailed in companion document:** [`02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md`](02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md).
- Evaluates 100,000 Jump-Diffusion price paths across 8 candidate `[t_L, t_U]` LP ranges in **$3.75\text{ ms}$** using 1-cycle logarithmic tick addition in **AMD Vitis HLS**.

### Use Case 2: Local "CME/Nasdaq-Style" Binary UDP Tick-to-Trade Hardware Lab
Use your Mac/Linux host as the "Exchange Gateway" (stripping away TLS and JSON) and transmit a **fixed 128-byte binary UDP packet (`MSG_DONTWAIT`)** out of [`RedisSink::emit_fast()`](../../evm_listener/src/sink/redis_sink.cpp#L278-L294) over an Ethernet cable directly into your FPGA board:

```
[Mac / Linux Host]                                        [Physical FPGA Board on Desk]
┌──────────────────────────────┐                          ┌────────────────────────────────────────┐
│ 1. market-data-streamer      │ 128-Byte Raw Binary UDP  │ 1. Pure Hardware UDP/IPv4 Parser       │
│    + evm_cpp_listener        │ ───────────────────────► │    (Extracts prices at fixed offsets)  │
│    (Handles wss:// TLS+JSON) │   (Local Ethernet Wire)  │ 2. Fixed-Point EMA + Spread Calculator │
│                              │                          │ 3. Quantized hls4ml RL Actor (DSP)     │
│ 2. Execution / Logger        │ ◄─────────────────────── │ 4. Fires 32-Byte Binary Order Packet   │
└──────────────────────────────┘  Order Trigger (<300 ns) └────────────────────────────────────────┘
```

#### Zero-Allocation 128-Byte Binary UDP Struct & `RedisSink::emit_fast()` Hook

```cpp
#pragma once
#include <cstdint>
#include <sys/socket.h>
#include <netinet/in.h>

#pragma pack(push, 1)
// Fixed 128-byte wire struct (CME SBE / Nasdaq ITCH style — zero pointers or heap strings)
struct FastEventUdpPacket {
    std::uint32_t magic{0x45564D4C};       // "EVML"
    std::uint16_t schema_version{1};
    std::uint16_t event_type{0};           // 1 = Log, 2 = Revert
    std::uint32_t chain_id{8453};          // Base = 8453
    std::uint32_t log_index{};
    std::uint64_t block_number{};
    std::uint64_t block_timestamp_s{};
    std::uint64_t observed_ms{};
    std::uint64_t emitted_ns{};            // High-res nanosecond timestamp
    std::uint8_t  contract_addr[20]{};     // Raw 20-byte EVM address
    std::uint8_t  topic0[32]{};            // Raw 32-byte event signature hash
    std::uint8_t  raw_data_prefix[32]{};   // First 32 bytes of ABI data (e.g. amount0)
};
#pragma pack(pop)
static_assert(sizeof(FastEventUdpPacket) == 128, "Must be exact 2-cache-line UDP payload");
```

Inside [`RedisSink::emit_fast(pipeline::Event e)`](../../evm_listener/src/sink/redis_sink.cpp#L278-L294), fire the non-blocking UDP datagram ($\sim 1.5\ \mu\text{s}$) in parallel with the durable Redis stream:

```cpp
void RedisSink::emit_fast(pipeline::Event e) {
    // 1. Hot-bypass: fire non-blocking binary UDP datagram (~1.5 µs, never blocks Asio loop)
    if (udp_fd_ >= 0) {
        FastEventUdpPacket pkt = pack_binary_udp(e);
        ::sendto(udp_fd_, &pkt, sizeof(pkt), MSG_DONTWAIT,
                 reinterpret_cast<const sockaddr*>(&udp_dest_), sizeof(udp_dest_));
    }

    // 2. Existing warm path: enqueue to Redis `evml:{shard}:fast` for Parquet Archiver & ClickHouse
    if (mode_.load() != SinkMode::Connected || saturated()) {
        ++fast_dropped_total_;
        return;
    }
    // ... existing mutex + queue_.emplace_back(FastEmitItem) ...
}
```

| Hot-Path Delivery Method from `emit_fast()` | Producer-to-Consumer Latency | Best Use Case |
| :--- | :--- | :--- |
| **1. Current Redis Stream (`evml:{shard}:fast`)** | **$50\text{–}150\ \mu\text{s}$** | Durable fan-out, consumer groups, Tier 1 Parquet Archiver, ClickHouse |
| **2. C++ Non-Blocking Binary UDP (`MSG_DONTWAIT`)** | **$2\text{–}5\ \mu\text{s}$** | Streaming fixed 128-byte binary structs to your FPGA board or LAN servers |
| **3. C++ Shared-Memory Ring (`shm_open` + `mmap`)** | **$0.1\text{–}0.3\ \mu\text{s}$ ($100\text{–}300\text{ ns}$)** | Feeding a C++ execution / ML inference bot running on the **same server** |

### Use Case 3: "DeFi Hardware Firewall" — Silicon-Enforced Risk Gate & Transaction Signer
Software private keys in `.env` or cloud memory can be drained if a server is compromised or an ML model outputs a bad parameter. Silicon gates cannot be bypassed by root attackers:
1. The host CPU sends an unsigned EVM transaction (`to`, `value`, `calldata`) to the FPGA.
2. **Hardcoded Silicon Policy Engine (LUTs):** Before signing, FPGA logic verifies:
   - `to` strictly matches the hardcoded 20-byte Aerodrome Slipstream Router/Vault address (physically impossible to sign an arbitrary `transfer()` to an attacker address).
   - Swap slippage / tick offset is within $\pm 50\text{ bps}$ of the latest `pool_fair_mid`.
   - Rate limit ($\le N$ rebalances per hour) has not been exceeded.
3. Only if all hardware gates pass does the FPGA compute the **`Keccak-256` digest + `secp256k1` ECDSA signature** using a key isolated inside FPGA BRAM/eFUSE.

### Use Case 4: EVM `Keccak-256` & `CREATE2` Smart Contract / Uniswap v4 Hook Miner
- **Why FPGAs Crush CPUs at `Keccak-256`:** `Keccak-256` consists entirely of 64-bit bitwise `XOR`, `AND`, `NOT`, and bit-rotations with zero memory lookups.
- **DeFi Application:** Mining `CREATE2` salts (`keccak256(0xff ++ deployer ++ salt ++ initCodeHash)`) for **Uniswap v4 Hook permission bitmasks** or **leading-zero gas-optimized vault addresses** at **$150\text{M}\text{–}300\text{M}\text{ hashes/sec}$** on a $5\text{W}$ FPGA.

---

## Annex: Research & References

- **Quantized FPGA Inference (`hls4ml` & `FINN`):**
  - Duarte et al., *"Fast inference of deep neural networks in FPGAs for particle physics"* (`hls4ml`), Journal of Instrumentation (JINST) — details sub-microsecond wire-attached FPGA inference and SRAM limits.
  - Umuroglu et al. (Xilinx Research), *"FINN: A Framework for Fast, Scalable Binarized Neural Network Inference"* — explains why FPGA LUT/DSP constraints require `INT1`–`INT8` fixed-point quantization.
- **Reinforcement Learning for Automated Market Making:**
  - Spooner et al., *"Market Making via Reinforcement Learning"*, AAMAS — actor-critic RL policies for inventory-aware market making using compact MLP networks.
