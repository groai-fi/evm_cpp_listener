# Parallel Fixed-Point Monte Carlo Simulator for Concentrated Liquidity (`[t_L, t_U]`) Tick Ranges on FPGA

> **Bottom Line Up Front (BLUF)**:
> To build a hardware-accelerated **Parallel Fixed-Point Monte Carlo LP Tick Range Simulator**, you do not need paid IP cores or raw Verilog. Using AMD's free **Vitis HLS + Vivado** toolchain and the open-source **AMD Vitis Quantitative Finance Library (`xf_fintech`, Apache 2.0)**, you can write the entire simulator in **C++ using fixed-point types (`ap_fixed<32, 16>`)**.
> Because Uniswap V3 and Aerodrome Slipstream ticks ($P(k) = 1.0001^k$) are **already in logarithmic space**, exponential Geometric Brownian Motion + Jump-Diffusion simplifies to **1-cycle fixed-point addition** inside FPGA DSP slices—evaluating **100,000 simulated paths across 8 candidate `[t_L, t_U]` LP ranges in $3.75\text{ milliseconds}$**.

---

## 1. The Mathematical Secret: Why LP Ticks Are Tailor-Made for FPGAs

In traditional stock or options Monte Carlo, simulating Geometric Brownian Motion requires computing a costly floating-point exponential $\exp(\cdot)$ at every time step:
$$S_{t+\Delta t} = S_t \cdot \exp\!\left(\left(\mu - \frac{1}{2}\sigma^2\right)\Delta t + \sigma\sqrt{\Delta t}\,Z_t\right)$$

In Aerodrome Slipstream / Uniswap V3, price $P$ and tick $k$ are linked by $P(k) = 1.0001^k$, which means **tick $k$ is already in log-price space**:
$$k = \frac{\ln(P)}{\ln(1.0001)}$$

Because $\ln(P_{t+\Delta t}) = \ln(P_t) + \text{drift} + \text{volatility} \cdot Z_t$, **exponential price motion becomes pure fixed-point addition in tick space**:

$$k_{t+1} = k_t + \mu_{\text{tick}} + \sigma_{\text{tick}} \cdot Z_t + J_t$$

* **Engineering translation with real numbers:**
  Suppose Base `EURC/USDC` is at tick $k_0 = 815$ ($P \approx 1.0849$), 1-minute drift (from `market-data-streamer` `3A/3B` lead-lag basis) is $\mu_{\text{tick}} = +0.02$ ticks, 1-minute realized tick volatility is $\sigma_{\text{tick}} = 1.50$ ticks, and the hardware Gaussian RNG draws $Z_t = -1.20$ (with zero jump $J_t = 0$).
  The next tick is simply:
  $$k_1 = 815 + 0.02 + (1.50 \times -1.20) = 813.22$$
  Inside an FPGA DSP48 slice using 32-bit fixed-point (`ap_fixed<32, 16>`), this multiply-accumulate (`MAC`) executes in **1 single clock cycle ($4\text{–}5\text{ nanoseconds}$)** with zero floating-point units and zero `exp()` lookup tables.

---

## 2. Required Toolchain, Free Open-Source IP Cores & Hardware Interface

| Component | Exact Tool / IP Core | Cost | What It Does |
| :--- | :--- | :--- | :--- |
| **1. C++-to-FPGA Compiler** | **AMD Vitis HLS + Vivado ML Standard (WebPACK)** | **Free ($0)** | Compiles C++ functions (`ap_fixed<32,16>`) with `#pragma HLS PIPELINE` & `#pragma HLS UNROLL` directly into FPGA hardware gates (`RTL` IP core). |
| **2. Financial & RNG IP Library** | **[AMD Vitis Quantitative Finance Library](https://github.com/Xilinx/Vitis_Libraries/tree/main/quantitative_finance)** (`xf_fintech`) | **Free (Open Source Apache 2.0)** | Provides ready-made C++ HLS hardware blocks for **Mersenne Twister / Sobol RNGs**, **Box-Muller / Inverse Cumulative Normal ($Z \sim \mathcal{N}(0,1)$)**, and **Jump-Diffusion Monte Carlo pipelines**. |
| **3. Bus / Network Interface IP** | **Xilinx `AXI4-Lite` + `AXI-DMA`** (on Zynq/Versal) or **UDP/UART** (on Artix/Spartan) | **Free (Built into Vivado)** | Lets the host CPU write live parameters (`current_tick`, `mu_drift`, `sigma_vol`, `jump_prob`, candidate `[t_L, t_U]` bands) into FPGA registers and read back simulation results in microseconds. |

---

## 3. Parallel Hardware Pipeline (`Vitis HLS` C++ Kernel)

Instead of simulating one path at a time like a CPU loop, use `#pragma HLS UNROLL factor=32` in **Vitis HLS** to instantiate **32 independent tick-path engines** side-by-side on the FPGA silicon, all stepping forward simultaneously on every clock cycle ($200\text{ MHz}$):

```cpp
#include <ap_fixed.h>
#include <ap_int.h>

// 32-bit signed fixed-point: 16 integer bits (ticks -32768..+32767), 16 fractional bits
using tick_fp_t = ap_fixed<32, 16>;

// Candidate LP range [tick_lower, tick_upper] to evaluate
struct CandidateBand {
    tick_fp_t tick_lower;
    tick_fp_t tick_upper;
};

// Single clock-cycle step across 32 parallel Monte Carlo paths & 8 candidate LP bands
void step_32_paths(
    tick_fp_t current_tick[32],
    const tick_fp_t z_normal[32],     // From xf_fintech Gaussian RNG (1 per lane per cycle)
    tick_fp_t mu_drift,
    tick_fp_t sigma_vol,
    const CandidateBand bands[8],
    ap_uint<32> steps_in_range[32][8],
    ap_uint<1>  breached_flag[32][8]
) {
#pragma HLS INLINE
#pragma HLS ARRAY_PARTITION variable=current_tick complete
#pragma HLS ARRAY_PARTITION variable=bands complete

    // All 32 paths and all 8 candidate LP bands evaluate in 1 SINGLE CLOCK CYCLE (5 ns)!
    PATHS_LOOP: for (int p = 0; p < 32; ++p) {
#pragma HLS UNROLL
        tick_fp_t next_k = current_tick[p] + mu_drift + (sigma_vol * z_normal[p]);
        current_tick[p] = next_k;

        BANDS_LOOP: for (int b = 0; b < 8; ++b) {
#pragma HLS UNROLL
            bool inside = (next_k >= bands[b].tick_lower) && (next_k <= bands[b].tick_upper);
            if (inside) {
                steps_in_range[p][b] += 1;
            } else {
                breached_flag[p][b] = 1; // Track first-passage exit out of range
            }
        }
    }
}
```

### Hardware Throughput & Latency Math
- **32 parallel paths $\times$ 8 candidate `[t_L, t_U]` bands = 256 band evaluations per clock cycle.**
- At $200\text{ MHz}$, running **100,000 Monte Carlo paths** over a **240-step horizon** (e.g., 4 hours at 1-minute steps) across **8 candidate LP ranges simultaneously** takes:
  $$\frac{100,000 \text{ paths} \times 240 \text{ steps}}{32 \text{ parallel lanes} \times 200,000,000 \text{ Hz}} = \mathbf{3.75\text{ milliseconds}}$$
- Every $3.75\text{ ms}$, the FPGA returns the complete risk and reward distribution across **8 candidate LP range widths** over 100,000 simulated futures.

---

## 4. Five Strategy Reliability Metrics Computed in Parallel

By attaching lightweight hardware accumulators and a Block RAM (BRAM) lookup table to the 32 parallel tick pipelines, the FPGA computes **5 critical LP risk and optimization metrics** simultaneously at zero extra clock latency:

### 4.1 Expected Time-in-Range ($\mathbb{E}[\text{TIR}]$) $\rightarrow$ Maximizes Fee & `AERO` Yield
- **What it measures:** The exact fraction of time over the next $N$ hours that price remains inside $[t_L, t_U]$ earning swap fees and `AERO` gauge emissions (`steps_in_range / total_steps`).
- **Strategy impact:** Prevents selecting an overly tight 2-tick range that advertises high instantaneous APR but exits range within 12 minutes.

### 4.2 First-Passage Touch Probability ($\Pr(\text{Rebalance})$) $\rightarrow$ Minimizes Rebalance Gas & Swap Slippage
- **What it measures:** The exact probability (`breached_flag == 1`) that the price path hits the lower or upper rebalance trigger before the target holding horizon expires.
- **Strategy impact:** Enforces a positive expected-value gate where a narrow range is chosen only if:
  $$\mathbb{E}[\text{Fees Earned}] > \Pr(\text{Breach}) \times (\text{Gas Cost} + \text{Swap Slippage})$$

### 4.3 Impermanent Loss Tail Risk ($\text{CVaR}_{95\%}$ / $\text{CVaR}_{99\%}$) via BRAM Lookup Table
- **What it measures:** At the terminal step $T$ of each path, the FPGA uses the final tick $k_T$ as an address into a pre-computed **4,096-entry Block RAM (BRAM) Lookup Table** storing the exact Uniswap V3 / Aerodrome Slipstream LP value formula:
  $$V_{\text{LP}}(k_T) = L \cdot \left(\sqrt{P(k_T)} - \sqrt{P(t_L)}\right) + L \cdot P(k_T)\left(\frac{1}{\sqrt{P(k_T)}} - \frac{1}{\sqrt{P(t_U)}}\right)$$
- **Strategy impact:** Computes the worst-case 5% and 1% tail Impermanent Loss ($\text{CVaR}_{95\%}$, $\text{CVaR}_{99\%}$) in **1 clock cycle** without computing square roots on the CPU.

### 4.4 Merton Jump-Diffusion & Stablecoin Depeg Stress-Testing
- **What it measures:** Real FX and crypto markets exhibit discontinuous jumps during macro releases (CPI/ECB/Fed) or stablecoin depeg events. Adding a uniform RNG comparator (`if (u_rand < jump_prob) next_k += jump_size;`) inside the unrolled loop costs **zero additional clock cycles**.
- **Strategy impact:** Automatically widens `[t_L, t_U]` or halts narrow rebalancing when `market-data-streamer` reports elevated cross-venue `dispersion_bps` or `depeg_warning`.

### 4.5 Asymmetric Skew Optimization (Inventory Mean-Reversion)
- **What it measures:** When the LP vault holds skewed inventory (e.g., $80\%$ `EURC` / $20\%$ `USDC`) and `market-data-streamer` shows `pool_fair_mid` (`3B`) is $+4\text{ bps}$ above the Base pool tick, the 8 candidate bands can include **asymmetric skews** (e.g., $[-3\text{ ticks}, +9\text{ ticks}]$ vs. $[-6\text{ ticks}, +6\text{ ticks}]$).
- **Strategy impact:** Selects the exact asymmetric tick band that maximizes probability of naturally offloading excess inventory into incoming swap flow without paying taker spread.

---

## 5. End-to-End Integration with `market-data-streamer` & `evm_cpp_listener`

```
┌──────────────────────────────────┐       ┌──────────────────────────────────┐
│      market-data-streamer        │       │         evm_cpp_listener         │
│ • north_star_mid (3A)            │       │ • On-chain Swap / Mint / Burn    │
│ • pool_fair_mid (3B) & basis_ema │       │ • Current pool tick & liquidity  │
│ • dispersion_bps & depeg_warning │       │   (sqrtPriceX96, tick)           │
└────────────────┬─────────────────┘       └────────────────┬─────────────────┘
                 │                                          │
                 └────────────────────┬─────────────────────┘
                                      ▼
            ┌───────────────────────────────────────────────────┐
            │     C++20 Strategy Controller (Host CPU)          │
            │  Computes rolling O(1) realized volatility σ_tick │
            │  & lead-lag drift μ_tick = (fair_tick - pool_tick)│
            └────────────┬─────────────────────────▲────────────┘
                         │ Send 6 params           │ Return 8-band risk table
                         │ (UDP / AXI4-Lite)       │ in 3.75 ms (TIR%, Breach%, CVaR)
                         ▼                         │
            ┌───────────────────────────────────────────────────┐
            │    FPGA Parallel Fixed-Point Monte Carlo Core     │
            │  • 32x Unrolled xf_fintech MT19937 + ICN RNGs     │
            │  • 32x 1-Cycle ap_fixed<32,16> Tick Path Engines  │
            │  • 8x Candidate [t_L, t_U] Evaluators + BRAM LUT  │
            └───────────────────────────────────────────────────┘
```

---

## Annex: References & Open-Source Code to Start From

- **AMD Vitis Quantitative Finance Library (Open-Source C++ HLS):**
  - Repository: `https://github.com/Xilinx/Vitis_Libraries/tree/main/quantitative_finance`
  - Key headers in `L1/include/xf_fintech/`:
    - `rng.hpp` & `mt19937IcnRng.hpp` — Pipelined Mersenne Twister (`MT19937`) + Inverse Cumulative Normal (`ICN`) Gaussian generator producing $Z \sim \mathcal{N}(0,1)$ every clock cycle (`II=1`).
    - `mc_engine.hpp` — Multi-path pipelined Monte Carlo valuation template.
- **Concentrated Liquidity Math in Tick Space:**
  - Adams et al., *"Uniswap v3 Core Whitepaper"* — Section 6 (Logarithmic Tick Spacing $P(i) = 1.0001^i$ and virtual reserves liquidity formulas).
- **Companion Architecture Document:**
  - See [`01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md`](01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md) for the broader FPGA vs. CPU/GPU comparison, `RedisSink::emit_fast()` 128-byte binary UDP tap, DeFi hardware firewall, and `Keccak-256` `CREATE2` miner.
