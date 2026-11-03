# Yearn v3 Tokenized Strategy: Aerodrome Spot Price-Parity Simulator on FPGA

> **Bottom Line Up Front (BLUF)**:
> A **Yearn v3 (`ERC-4626` Tokenized Strategy)** that buys spot discounts on Aerodrome with a limit price (`sqrtPriceLimitX96`) and sells when the Aerodrome pool converges back to the CEX price (`3B pool_fair_mid`) is **even simpler to simulate on an FPGA than continuous LP range rebalancing**.
> In hardware terms, the strategy reduces to a **2-State Holding Finite State Machine (`VAULT_IDLE_USDC` $\leftrightarrow$ `VAULT_HOLDING_SPOT`)** governed by fixed-point price comparators. Using **AMD Vitis HLS (`ap_fixed<32, 16>`)**, the FPGA simulates **100,000 adversarial Jump-Diffusion + Lead-Lag paths across 16 candidate entry/exit parameter sets in $\sim 3.5\text{ milliseconds}$**, tracking exact Yearn v3 Share Price (`PPS`), `totalAssets()` growth, Win/Loss ratio, and Max Peak-to-Trough Drawdown.

---

## 1. Three Real-World On-Chain Mechanics the FPGA Must Model

To prevent the simulator from overestimating profits, the FPGA kernel models three physical constraints of executing a Yearn v3 strategy on Base L2 Aerodrome Spot:

### 1.1 The Round-Trip Swap Fee, Price Impact & `tend()` Gas Hurdle
- When the off-chain keeper calls `tend()` on the Yearn v3 `TokenizedStrategy` to buy `EURC` on Aerodrome Spot (using `sqrtPriceLimitX96` to enforce the limit price) and later calls `tend()` to sell `EURC` back to `USDC` at CEX parity, the vault pays the Aerodrome pool fee **twice** (entry + exit), plus limit-fill slippage and Base L2 gas.
- **Concrete math:** If the Aerodrome Slipstream pool fee is $4\text{ bps}$ ($4\text{ ticks}$) per swap, round-trip fees alone cost **$8\text{ bps}$**. Buying a $5\text{ bps}$ CEX-to-Aerodrome disparity loses money even with a $100\%$ mean-reversion rate. The entry disparity $\theta_{\text{buy}}$ must satisfy:
  $$\theta_{\text{buy}} = M_t - X_t > 2 \times \text{Fee}_{\text{pool}} + \text{Slippage}_{\text{limit}} + \text{Gas}_{\text{tend}} + \text{SafetyMargin}$$
  *(e.g., $\theta_{\text{buy}} \ge 12\text{ to } 15\text{ ticks}$ for a $4\text{ bps}$ pool).*

### 1.2 The "Moving Target" Parity Trap (CEX Drops to Meet Aerodrome vs. Pool Bounces to CEX)
- Suppose off-chain CEX `EUR/USD` ($M_t$) is at `1.0850` and Aerodrome Spot ($X_t$) dumps to `1.0835` ($-14\text{ bps}$ discount). The Yearn v3 vault buys at `1.0835` expecting $X_t$ to bounce back up to $M_t$.
- **Failure Mode:** What if the on-chain seller was informed, and 5 seconds later CEX $M_t$ drops to `1.0830`? Now $X_t \ge M_t$ (price parity is reached!), *but parity occurred below the vault's `1.0835` entry price*.
- **Hardware Solution:** The FPGA evaluates both exit policies in parallel:
  1. **Pure Parity Exit (`min_profit_ticks = -infinity`):** Sell as soon as $M_t - X_t \le \epsilon$, cutting the loss immediately when CEX moves down to meet the pool.
  2. **Profit-Floor Parity Exit (`min_profit_ticks >= 0`):** Require both $M_t - X_t \le \epsilon$ **and** $X_t - X_{\text{entry}} \ge \text{min\_profit\_ticks}$, backed by a hard **Time-Stop (`max_hold_steps`)** and **Stop-Loss (`stop_loss_ticks`)**.

### 1.3 Yearn v3 `totalAssets()` & Price-Per-Share (`PPS`) Drawdown Accounting
- In Yearn v3 (`ERC-4626`), vault health is measured by monotonic **Price Per Share (`PPS = totalAssets() / totalSupply()`)** and **Max Drawdown**.
- While the strategy is in `VAULT_HOLDING_SPOT`, any further drop in Aerodrome spot price $X_t$ reduces mark-to-market `totalAssets()` in `USDC` terms. The FPGA tracks peak-to-trough unrealized and realized `PPS` drawdown at every clock cycle across all 100,000 simulated paths.

---

## 2. Synthesizeable `Vitis HLS` C++ Kernel (`step_yearn_v3_parity_strategy`)

The following 1-cycle pipelined C++ HLS function plugs directly into the coupled Lead-Lag + Jump-Diffusion + Student-$t$ Particle Filter loop from [`03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md`](03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md):

```cpp
#include <ap_fixed.h>
#include <ap_int.h>
#include <stdint.h>

using fp_t = ap_fixed<32, 16>; // 16 integer bits (ticks), 16 fractional bits

enum VaultState : uint8_t {
    VAULT_IDLE_USDC    = 0, // 100% idle in base asset USDC
    VAULT_HOLDING_SPOT = 1  // Holding target asset (e.g. EURC) bought at discount; waiting for CEX parity
};

struct YearnParityConfig {
    fp_t buy_disparity_ticks;  // Min discount (M_t - X_t) to trigger buy, e.g. 12.0 ticks
    fp_t limit_slippage_cap;   // Max slippage on sqrtPriceLimitX96 fill
    fp_t exit_parity_tol;      // Sell when (M_t - X_t) <= exit_parity_tol, e.g. 1.0 tick
    fp_t min_profit_ticks;     // Profit floor: require trade_pnl >= min_profit_ticks on parity exit
    fp_t stop_loss_ticks;      // Emergency exit if trade_pnl <= -stop_loss_ticks
    uint16_t max_hold_steps;   // Time-stop: max blocks/steps to hold before forced exit
    fp_t swap_fee_ticks;       // One-way Aerodrome pool fee + price impact (e.g. 4.0 ticks)
    fp_t tend_gas_ticks;       // Base L2 gas cost of tend() call in tick-equivalents
    fp_t p_jump_abort_thresh;  // Oracle gate: block entry / force exit if P_jump >= thresh
};

void step_yearn_v3_parity_strategy(
    // Environment & Oracle inputs (from coupled Jump-Diffusion + Particle Filter):
    fp_t  cex_tick,            // Off-chain CEX fair price M_t (3B pool_fair_mid)
    fp_t  aero_tick,           // On-chain Aerodrome spot tick X_t
    fp_t  p_jump,              // Structural jump/depeg probability from Particle Filter Oracle
    // Yearn v3 Tokenized Strategy state registers:
    VaultState& state,
    fp_t& entry_tick,
    uint16_t& hold_steps,
    fp_t& vault_nav_ticks,     // Cumulative Yearn v3 Strategy PnL (totalAssets delta)
    fp_t& peak_nav_ticks,      // High-water mark for Max Drawdown calculation
    fp_t& max_drawdown_ticks,  // Worst peak-to-trough PPS drawdown across path
    uint32_t& win_count,
    uint32_t& loss_count,
    const YearnParityConfig& cfg
) {
#pragma HLS INLINE

    // Positive disparity means Aerodrome Spot is CHEAPER than CEX fair price
    fp_t disparity = cex_tick - aero_tick;

    if (state == VAULT_IDLE_USDC) {
        // Limit Buy Trigger:
        // 1. Discount (M_t - X_t) >= buy_disparity_ticks
        // 2. Oracle confirms this is a temporary liquidity dip, NOT a depeg (p_jump < thresh)
        bool should_buy = (disparity >= cfg.buy_disparity_ticks) &&
                          (p_jump < cfg.p_jump_abort_thresh);

        if (should_buy) {
            // Keeper calls tend() -> Aerodrome swap() with sqrtPriceLimitX96
            entry_tick      = aero_tick + cfg.swap_fee_ticks + cfg.limit_slippage_cap;
            vault_nav_ticks -= cfg.tend_gas_ticks;
            hold_steps      = 0;
            state           = VAULT_HOLDING_SPOT;
        }
    } else if (state == VAULT_HOLDING_SPOT) {
        hold_steps += 1;

        // Net exit price if sold on Aerodrome spot right now (minus exit swap fee)
        fp_t net_exit_tick = aero_tick - cfg.swap_fee_ticks;
        fp_t trade_pnl     = net_exit_tick - entry_tick;

        // Update live Mark-to-Market Yearn v3 NAV & Peak-to-Trough Max Drawdown
        fp_t live_nav = vault_nav_ticks + trade_pnl;
        if (live_nav > peak_nav_ticks) peak_nav_ticks = live_nav;
        fp_t dd = peak_nav_ticks - live_nav;
        if (dd > max_drawdown_ticks) max_drawdown_ticks = dd;

        // 3 Exit Conditions:
        // (A) Reached back to CEX parity (disparity closed) AND satisfies min_profit_ticks
        bool reached_cex_parity = (disparity <= cfg.exit_parity_tol) &&
                                  (trade_pnl >= cfg.min_profit_ticks);

        // (B) Hard Stop-Loss or Oracle Depeg Circuit Breaker
        bool hit_stop_loss = (trade_pnl <= -cfg.stop_loss_ticks) ||
                             (p_jump >= cfg.p_jump_abort_thresh);

        // (C) Time-Stop: position failed to mean-revert within max_hold_steps
        bool hit_time_stop = (hold_steps >= cfg.max_hold_steps);

        if (reached_cex_parity || hit_stop_loss || hit_time_stop) {
            vault_nav_ticks += trade_pnl - cfg.tend_gas_ticks;
            if (vault_nav_ticks > peak_nav_ticks) peak_nav_ticks = vault_nav_ticks;

            if (trade_pnl > 0) win_count += 1;
            else               loss_count += 1;

            state = VAULT_IDLE_USDC;
        }
    }
}
```

---

## 3. Parallel Parameter Sweep: Testing 16 Strategy Configurations in $3.5\text{ ms}$

By unrolling **32 parallel Monte Carlo paths $\times$ 16 `YearnParityConfig` candidates** (`512` strategy evaluations per clock cycle at $200\text{ MHz}$), a single $3.5\text{ ms}$ FPGA run optimizes the exact parameters of your Yearn v3 strategy:

| Parameter Swept on FPGA | Engineering Question Answered Before Risking Vault TVL |
| :--- | :--- |
| **1. `buy_disparity_ticks` ($8, 10, 12, 15, 20, 30\text{ bps}$)** | What is the minimum CEX-to-Aerodrome discount where mean-reversion gains reliably beat $2\times$ Aerodrome swap fees + slippage? |
| **2. `min_profit_ticks` ($-5, 0, +2, +5\text{ bps}$)** | When CEX drifts down while holding `EURC`, is it better to exit immediately at the new CEX parity (small loss) or hold out for the original entry price? |
| **3. `max_hold_steps` ($5\text{m}, 15\text{m}, 60\text{m}, 240\text{m}$)** | Given Aerodrome's arbitrage decay rate $\kappa_{\text{arb}}$, how long does a real liquidity dip take to close? If it hasn't closed in $15\text{ minutes}$, should `tend()` cut the position? |
| **4. `p_jump_abort_thresh` (With vs. Without Oracle Gate)** | How much `PPS` drawdown does the **Student-$t$ Particle Filter / Hawkes Oracle** prevent during a real depeg or macro shock compared to a blind limit-order strategy? |
| **5. Maker Range-Order vs. Taker Limit-Swap (`swap_fee_ticks`)** | How much higher is Yearn v3 APY if `tend()` places a 1-tick concentrated LP range order (`swap_fee_ticks = -1.0` maker rebate) vs. a taker swap (`swap_fee_ticks = +4.0` fee)? |

---

## 4. End-to-End Integration with Your Yearn v3 Keeper Bot

```
┌──────────────────────────────────┐       ┌──────────────────────────────────┐
│      market-data-streamer        │       │         evm_cpp_listener         │
│ • CEX Fair Price M_t             │       │ • Aerodrome Spot Tick X_t        │
│   (3B pool_fair_mid)             │       │   (sqrtPriceX96, liquidity)      │
│ • Venue dispersion & depeg flags │       │ • Pool swap volume & tick deltas │
└────────────────┬─────────────────┘       └────────────────┬─────────────────┘
                 │                                          │
                 └────────────────────┬─────────────────────┘
                                      ▼
       ┌─────────────────────────────────────────────────────────────┐
       │         C++20 Yearn v3 Keeper & Parameter Calibrator        │
       │ 1. Calibrates rolling (κ_arb, σ_cex, σ_pool, jump_prob)     │
       │ 2. Sends 16 candidate YearnParityConfig structs to FPGA     │
       └──────────────┬───────────────────────────────▲──────────────┘
                      │ AXI4-Lite / 128B UDP          │ 3.5 ms: Returns winning
                      ▼                               │ config (Max NAV / Min DD)
       ┌──────────────────────────────────────────────┴──────────────┐
       │        FPGA Closed-Loop Yearn v3 Strategy Simulator         │
       │ • Simulates 100,000 Jump-Diffusion + Lead-Lag paths         │
       │ • Evaluates 16 YearnParityConfig thresholds in parallel     │
       │ • Computes Expected PPS Growth, Win Rate %, and Max DD      │
       └─────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
       ┌─────────────────────────────────────────────────────────────┐
       │       Base L2: Yearn v3 TokenizedStrategy (ERC-4626)        │
       │ • Keeper triggers tend() when live (M_t - X_t) >= optimal   │
       │   buy_disparity_ticks and P_jump < p_jump_abort_thresh      │
       │ • Executes Aerodrome swap(..., sqrtPriceLimitX96)           │
       └─────────────────────────────────────────────────────────────┘
```

---

## Annex: Related Documents in `doc/fpga_use_case/`

- [`01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md`](01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md) — Hardware vs. EVM/CEX latency analysis, `RedisSink::emit_fast()` 128-byte binary UDP tap, DeFi hardware firewall, and `Keccak-256` `CREATE2` miner.
- [`02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md`](02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md) — Logarithmic tick-space derivation, AMD `Vitis Quantitative Finance Library` (`xf_fintech`) setup, and 32-lane `[t_L, t_U]` LP range evaluator.
- [`03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md`](03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md) — DGN (`EURC/USDC`) vs. Pegged (`USDT/USDC`) quantitative foundation, Ornstein-Uhlenbeck + Merton Jump-Diffusion, 1-cycle Markovian Hawkes intensity ($\lambda_{\text{toxic}}$), Student-$t$ BRAM LUT Particle Filter ($P_{\text{jump}}$), and the 3-State FSM (`State A / B / C`) simulator.
