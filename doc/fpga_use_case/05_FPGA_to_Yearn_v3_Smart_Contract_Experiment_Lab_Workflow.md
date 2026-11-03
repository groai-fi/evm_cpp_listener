# 6-Stage Quant & FPGA Experiment Lab Workflow: From Parquet Calibration to Yearn v3 Smart Contract Deployment

> **Bottom Line Up Front (BLUF)**:
> Always prove strategy profitability and tail-risk survival in simulation **before** writing and deploying your Yearn v3 (`ERC-4626` `TokenizedStrategy`) Solidity contract, governed by two core engineering rules:
> 1. **Use the FPGA to Decide the Solidity *Mechanism* and *Safety Bounds*, Not to Hardcode Thresholds into Solidity:** Bake the winning execution mechanism (e.g., Taker Limit-Swap via `Router.swap(sqrtPriceLimitX96)` vs. Maker 1-Tick Concentrated LP Range Order via `CLPool.mint`/`burn`) and hard safety ceilings (`maxSlippageBps = 50`, whitelisted pool address) into Solidity. Keep operational thresholds (`buy_disparity_ticks`, `exit_parity_tol`, `max_hold_steps`) dynamic in keeper calldata or configurable strategy storage so the FPGA can re-optimize them as market volatility ($\sigma$) and arbitrage speed ($\kappa_{\text{arb}}$) evolve.
> 2. **Always Run `Vitis HLS C-Simulation (csim)` on Your CPU First (2 Seconds) Before Synthesizing to FPGA Gates (30 Minutes):** Because the FPGA simulator is written in C++ (`ap_fixed<32, 16>`), Vitis HLS compiles and executes the exact hardware code on a standard CPU in 2 seconds for rapid debugging against historical Parquet data before generating the physical FPGA bitstream.

---

## 1. End-to-End 6-Stage Experiment Lab Workflow

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: EMPIRICAL CALIBRATION (CPU: Parquet + DuckDB — Now)                           │
│ Join `market-data-streamer` (CEX M_t) + `evm_cpp_listener` (Aerodrome X_t)             │
│ Extract real Base pool constants: κ_arb, σ_cex, σ_pool, jump_prob, Hawkes (α, β), ν   │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: VITIS HLS C-SIMULATION (CPU — 2-Second Debug Loop)                            │
│ Write `step_yearn_v3_parity_strategy()` in C++ (`ap_fixed<32,16>`)                     │
│ Verify against historical Parquet replay & 1,000 MC paths before hardware synthesis    │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: FPGA HARDWARE "WIND TUNNEL" (FPGA Board — 3.5 ms per 100k Paths x 16 Configs) │
│ Sweep Taker Limit-Swap vs. Maker 1-Tick LP Order across 100,000 Jump-Diffusion paths   │
│ GO / NO-GO GATE: Only write Solidity if Expected Net APY > Hurdle & Max DD < Limit!    │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4: YEARN V3 SOLIDITY CONTRACT DESIGN (Foundry — Solidity)                        │
│ Build `TokenizedStrategy` using the winning execution mechanism from Stage 3           │
│ Hardcode safety invariants; keep entry/exit thresholds dynamic via Keeper calldata     │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 5: BASE MAINNET FORK TEST (Foundry `anvil --fork-url`)                           │
│ Replay Stage 3 signals against real on-chain Aerodrome Slipstream contracts            │
│ Verify exact `uint256` / `sqrtPriceX96` rounding, gas cost, and `report()` PPS math   │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 6: LIVE SHADOW MODE ──► CAPPED-TVL DEPLOYMENT ON BASE L2                         │
│ Keeper + FPGA re-optimizes thresholds every 60s as live market volatility shifts       │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Detailed Breakdown of Each Stage

### Stage 1: Extract Real Market Constants from Your Parquet Data Lake (No Guessing)
A Monte Carlo simulator fed with synthetic guesses produces unreliable thresholds. Use the live streams from [`evm_cpp_listener`](../System_Modules_and_Architecture.md) and `market-data-streamer` archived in your **Tier 1 Parquet Data Lake** ([`Two_Tier_Parquet_DuckDB_ClickHouse_Architecture.md`](../data_lake/Two_Tier_Parquet_DuckDB_ClickHouse_Architecture.md)) to calibrate the real parameters of Base `EURC/USDC`:

```sql
-- Example DuckDB Calibration Query Joining CEX Fair Price (M_t) and Aerodrome Swaps (X_t)
SELECT
    approx_quantile(abs(cex_tick - aero_tick), 0.95) AS p95_disparity_ticks,
    stddev(aero_tick_delta)                          AS sigma_pool_ticks,
    avg(CASE WHEN abs(aero_tick_delta) > 10 THEN 1.0 ELSE 0.0 END) AS empirical_jump_prob
FROM joined_cex_aerodrome_ticks
WHERE dt >= '2026-11-01';
```

- **Mean-Reversion Speed ($\kappa_{\text{arb}}$):** Measure the half-life (in seconds/blocks) required for a $\ge 12\text{ bps}$ CEX-to-Aerodrome disparity ($M_t - X_t$) to decay by $50\%$.
- **Jump Intensity ($p_{\text{jump}}, J_t$):** Measure the empirical frequency and magnitude of single-block whale swaps exceeding $10\text{ ticks}$.
- **Hawkes Self-Excitation ($\alpha, \beta$) & Student-$t$ Tail ($\nu$):** Fit trade arrival clustering and residual kurtosis on historical swaps.

### Stage 2: Write the C++ HLS Kernel & Verify in `csim` (On Your Host CPU)
Before synthesizing to hardware gates, write your strategy kernel ([`04_FPGA_Yearn_v3_Aerodrome_Price_Parity_Strategy_Simulator.md`](04_FPGA_Yearn_v3_Aerodrome_Price_Parity_Strategy_Simulator.md)) and a C++ testbench (`main_tb.cpp`):
1. **Historical Replay Verification:** Feed 7 days of recorded $(M_t, X_t)$ ticks from Parquet into `step_yearn_v3_parity_strategy()` in C++ to confirm the fixed-point `ap_fixed<32, 16>` arithmetic matches reference floating-point accounting with zero overflow.
2. **Fast CPU Iteration:** Execute `vitis_hls -f run_csim.tcl` (runs in **$\sim 2\text{ seconds}$** on CPU).

### Stage 3: Run the FPGA Hardware Wind Tunnel (The Strategy Design Gate)
Once `csim` passes, run `csynth` in Vitis HLS and generate the Vivado bitstream for your physical FPGA board to execute 100,000-path parameter sweeps across stress regimes in **$3.5\text{ ms}$**.

**How Stage 3 Prevents Wasted Smart Contract Engineering:**
- *Scenario A (Taker Limit-Swap):* The FPGA sweep may show that paying $2 \times 4\text{ bps} = 8\text{ bps}$ round-trip taker swap fees on `EURC/USDC` requires a $\ge 14\text{ bps}$ disparity, which only occurs twice a week (yielding $2.1\%$ APY).
- *Scenario B (Maker 1-Tick Concentrated LP Range Order):* The same FPGA sweep tests `swap_fee_ticks = -1.0` (placing a 1-tick single-sided LP range order below spot that *earns* fees when filled) and reveals it triggers 15 times a day at a $6\text{ bps}$ disparity, yielding **$18.4\%$ APY with $0.4\%$ max drawdown**.
- **Result:** Before writing a single line of Solidity, you know whether your Yearn v3 contract should implement **Taker Swap (`Router.swap`)** or **Maker Range Orders (`CLPool.mint`/`burn`)**.

### Stage 4: Design the Yearn v3 Solidity Smart Contract (`TokenizedStrategy`)
Translate the verified Stage 3 design into Solidity while strictly separating **immutable safety invariants** from **dynamic FPGA-tuned thresholds**:

| Solidity Contract Layer | How It Is Designed from Stage 3 FPGA Findings |
| :--- | :--- |
| **Core Execution (`_deployFunds` / `_tend`)** | Implements the winning execution mechanism proven in Stage 3 (Aerodrome `swap` with `sqrtPriceLimitX96` or 1-tick LP `mint`/`burn`). |
| **Hard On-Chain Risk Gates (`immutable` / `require`)** | Enforces worst-case safety bounds proven in Stage 3 (`require(lossBps <= maxAllowedLossBps)`, whitelisted Aerodrome pool address, `onlyKeepers`, and emergency `_emergencyWithdraw`). |
| **Dynamic Strategy Parameters (Keeper / Storage)** | Exposes configurable parameters (`buyDisparityTicks`, `exitParityTolTicks`, `stopLossTicks`, `maxHoldSeconds`) or accepts `sqrtPriceLimitX96` directly in `tend()` calldata so the keeper can update thresholds dynamically without redeploying the contract. |

### Stage 5: Foundry Mainnet Fork Verification (`anvil --fork-url`)
While the FPGA validates statistical profitability across 100,000 stochastic futures, **Foundry validates exact EVM byte-level execution**:
- Run `forge test --fork-url $BASE_RPC_URL` replaying winning trade triggers from Stage 3 against the real deployed Aerodrome Slipstream pool on Base L2.
- Verify exact `uint256` / `sqrtPriceX96` fixed-point rounding, `ERC-4626` `report()` / `tend()` share price (`PPS`) accounting, and actual L2 gas consumption.

### Stage 6: Live Shadow Mode $\rightarrow$ Capped-TVL Production Loop
1. **72-Hour Shadow Mode:** Run your C++ keeper (`market-data-streamer` + `evm_cpp_listener`) + FPGA co-processor live against Base mainnet for 72 hours, logging simulated `tend()` triggers and paper PnL without broadcasting transactions.
2. **Capped-TVL Launch & Continuous Re-Optimization:** Deploy the Yearn v3 vault with an initial deposit cap ($\$1,000\text{–}\$5,000$). Every 60 seconds, as live market volatility $\sigma$ or Hawkes toxicity $\lambda_{\text{toxic}}$ shifts, the C++ keeper queries the FPGA for a fresh $3.5\text{ ms}$ 100,000-path parameter sweep and updates its active entry/exit thresholds automatically.

---

## 3. Lab Hardware & Software Setup Checklist

| Environment | Required Components | Status / Purpose |
| :--- | :--- | :--- |
| **1. Data & Keeper Host (Mac / Linux)** | • [`evm_cpp_listener`](../System_Modules_and_Architecture.md) + `market-data-streamer` + Redis 8.4<br>• **Tier 1 Parquet + DuckDB Archiver** ([`Parquet_Archiver_Implementation_Plan.md`](../data_lake/Parquet_Archiver_Implementation_Plan.md))<br>• **Foundry (`forge`, `anvil`, `cast`)** | Captures live Base + CEX ticks, calibrates Stage 1 SDE parameters in DuckDB, and tests Stage 4/5 Yearn v3 Solidity contracts. |
| **2. FPGA Synthesis Toolchain (Linux x86_64 PC / VM)** | • **AMD Vitis HLS + Vivado ML Standard (Free WebPACK)**<br>• **[`Xilinx/Vitis_Libraries/quantitative_finance`](https://github.com/Xilinx/Vitis_Libraries/tree/main/quantitative_finance)** (`xf_fintech`)<br>• USB-JTAG + Local Ethernet link to FPGA board | Runs 2-second Stage 2 `csim` on CPU and compiles Stage 3 hardware bitstreams for the physical FPGA board. |

---

## Annex: Complete `doc/fpga_use_case/` Index

1. [`01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md`](01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md) — Hardware vs. EVM/CEX latency analysis, `RedisSink::emit_fast()` 128-byte binary UDP tap, DeFi hardware firewall, and `Keccak-256` `CREATE2` miner.
2. [`02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md`](02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md) — Logarithmic tick-space derivation, AMD `Vitis Quantitative Finance Library` (`xf_fintech`) setup, and 32-lane `[t_L, t_U]` LP range evaluator.
3. [`03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md`](03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md) — DGN (`EURC/USDC`) vs. Pegged (`USDT/USDC`) quantitative foundation, Ornstein-Uhlenbeck + Merton Jump-Diffusion, 1-cycle Markovian Hawkes intensity ($\lambda_{\text{toxic}}$), Student-$t$ BRAM LUT Particle Filter ($P_{\text{jump}}$), and the 3-State FSM (`State A / B / C`) simulator.
4. [`04_FPGA_Yearn_v3_Aerodrome_Price_Parity_Strategy_Simulator.md`](04_FPGA_Yearn_v3_Aerodrome_Price_Parity_Strategy_Simulator.md) — Yearn v3 (`ERC-4626`) Aerodrome spot limit-buy / CEX parity-sell FSM (`step_yearn_v3_parity_strategy` in `Vitis HLS`), round-trip fee hurdle, moving-target CEX parity rules, and 16-config parallel parameter sweep.
5. [`05_FPGA_to_Yearn_v3_Smart_Contract_Experiment_Lab_Workflow.md`](05_FPGA_to_Yearn_v3_Smart_Contract_Experiment_Lab_Workflow.md) — *(This document)* 6-stage operational workflow from Parquet parameter calibration and Vitis HLS `csim` to FPGA wind-tunnel sweeps, Yearn v3 Solidity contract architecture, and Foundry mainnet fork verification.
