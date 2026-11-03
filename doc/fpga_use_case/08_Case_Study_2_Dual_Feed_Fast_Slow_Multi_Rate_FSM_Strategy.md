# Case Study 2: Real-World Dual-Feed (Fast $25\text{ ms}$ + Slow $2\text{s}/22\text{s}$) Multi-Rate FSM Strategy on Base `EURC/USDC`

> **Bottom Line Up Front (BLUF)**:
> In real production, off-chain CEX ticks and on-chain Base L2 blocks do **not** arrive synchronously at the same clock rate, and on-chain execution is **not** instantaneous.
> Between `market-data-streamer` (`min_emit_interval_ms = 25`) and [`evm_cpp_listener`](../System_Modules_and_Architecture.md) ($2,000\text{ ms}$ Base blocks + $22\text{s}$ `safe` finality), there is an **80:1 clock-speed asymmetry** (80 CEX ticks occur during every single Base L2 block) and a **1-block ($2,000\text{ ms}$) order-in-flight execution delay** between detecting a discount on `evml:{base}:fast` and landing a keeper transaction on-chain.
> This case study details how to implement the **Dual-Feed (Fast + Slow) Multiplexer** in C++20 (`DualFeedPacket`) and how to program a **Multi-Rate `Vitis HLS` FPGA Simulator (`step_case_study_2_dual_feed`)** that steps the Fast CEX/Hawkes feed every cycle ($25\text{ ms}$) while stepping Base L2 blocks, in-flight keeper orders, and Slow Student-$t$ / Reorg checks every 80th cycle ($2,000\text{ ms}$).

---

## 1. Architecture of the Production Dual-Feed (Fast + Slow) System

In production, **Fast Data** and **Slow Data** have asymmetric authority over the 3-State FSM (`State A: Yield / Entry`, `State B: Toxicity Defense`, `State C: Circuit Breaker`):

```
====================================================================================================
FAST FEED (Every 25 ms CEX / 250 ms–2s Base Tip) ──► "INSTANT BRAKES & VELOCITY FILTER"
====================================================================================================
• Sources: `mds:{eur_usd}:stream` (25 ms north_star_mid 3A, spread, CEX trade bursts)
           `evml:{base}:fast`     (Unconfirmed tip Swap ticks & flashblock volume)
• Updates: Fast Oracle ──► Hawkes Intensity (λ_toxic) + 500ms CEX Velocity (v_cex)
• FSM Rule: Can IMMEDIATELY trigger State B (Skew/Block Entry) or State C (Evacuate) on unconfirmed
            data! Never waits 22s for `safe` finality when a whale dump or CEX crash is underway.

====================================================================================================
SLOW FEED (Every 2s Block / 22s Safe / 30m EMA)  ──► "GREEN LIGHT FOR CAPITAL ENTRY & REORG TRUTH"
====================================================================================================
• Sources: `mds:{eur_usd}:state`  (30m `basis_ema_bps` 3B, `depeg_warning`, `active_primary >= 2`)
           `evml:{base}:events`   (22s `safe` confirmed fills, `type="revert"` reorg retractions,
                                   Mint/Burn pool liquidity L, and Gauge 0x1f6c... Deposit/Withdraw)
• Updates: Slow Oracle ──► Student-t HHMM + Particle Filter (P_jump) + Confirmed Vault Inventory
• FSM Rule: Required to authorize State A (Deploying USDC into a new trade) and to roll back
            state if a `fast` tip fill is retracted by a `revert` event!
```

---

## 2. C++20 Dual-Feed Wire Protocol (`DualFeedPacket`)

To feed both live execution and the FPGA co-processor without mutex contention, the C++20 multiplexer emits a **tagged 128-byte binary UDP packet (`DualFeedPacket`)** where `feed_kind` distinguishes **Fast Ticks ($25\text{ ms}$)** from **Slow Confirmed Updates ($2\text{s}\text{–}22\text{s}$)**:

```cpp
#pragma once
#include <cstdint>

#pragma pack(push, 1)
enum class FeedKind : uint8_t {
    FAST_CEX_25MS       = 1, // From market-data-streamer (every 25 ms)
    FAST_BASE_TIP       = 2, // From evml:{base}:fast (every 2,000 ms block tip, finality="latest")
    SLOW_BASE_SAFE      = 3, // From evml:{base}:events (finality="safe", confirmed fill or revert)
    SLOW_MACRO_REGIME   = 4  // From market-data-streamer (30m basis_ema_bps, depeg_warning, leg fitness)
};

struct DualFeedPacket {
    uint32_t magic{0x4455414C};        // "DUAL"
    FeedKind feed_kind;                // 1=FAST_CEX, 2=FAST_TIP, 3=SLOW_SAFE, 4=SLOW_REGIME
    uint8_t  is_revert{0};             // 1 if evml:{base}:events emitted type="revert"
    uint8_t  depeg_warning{0};         // From market-data-streamer
    uint8_t  active_primary_venues{3}; // Must be >= 2 for FX_OPEN
    uint64_t timestamp_ms{};
    int32_t  north_star_tick_fp16{};   // 3A Fast CEX Mid (in Q16.16 fixed-point ticks)
    int32_t  pool_fair_tick_fp16{};    // 3B Slow Basis-Adjusted Fair Mid (Q16.16 ticks)
    int32_t  aero_pool_tick_fp16{};    // Pool 1 (0xc5E5... ts=1) tick (Q16.16)
    uint32_t trade_burst_usd{};        // 25ms CEX or Base swap volume for Hawkes λ_toxic
    uint8_t  padding[92]{};
};
#pragma pack(pop)
static_assert(sizeof(DualFeedPacket) == 128, "Exact 2-cache-line Dual-Feed packet");
```

---

## 3. Multi-Rate FPGA Kernel: Simulating the **80:1 Clock Ratio** & **1-Block Execution Delay**

Single-rate backtests overestimate profitability because they assume that when a $7\text{-tick}$ discount appears at Block $B$, the keeper fills at Block $B$'s exact price.

In **Case Study 2**, the `Vitis HLS` kernel enforces the true multi-rate physical timeline of Base L2:
- **Every Fast Cycle ($25\text{ ms}$):** CEX `north_star_mid` ($M_t$), the 500ms CEX Velocity filter (`cex_velocity_500ms`), and the **Fast Hawkes Oracle ($\lambda_{\text{toxic}}$)** step forward.
- **Every 80th Fast Cycle ($2,000\text{ ms}$ = 1 Base Block):**
  1. **In-Flight Order Settlement (`STATE_A_ORDER_IN_FLIGHT`):** Any `tendStrategy()` transaction submitted by the Keeper at the *previous* block (`80` CEX ticks ago) finally executes against the **current** block's `aero_pool_tick`—after $2,000\text{ ms}$ of CEX drift and competing MEV bot activity!
  2. **New Base Block Arrival (`FAST_BASE_TIP`):** `aero_pool_tick` steps forward with mean-reversion toward the Slow-adjusted `3B pool_fair_mid` ($M_t + \text{slow\_basis\_ema}$) plus on-chain whale dump shocks.
  3. **Slow Oracle Update:** Evaluates the Student-$t$ BRAM Lookup Table to update $P_{\text{jump\_slow}}$.

### Synthesizeable Multi-Rate Dual-Feed `Vitis HLS` Kernel (`step_case_study_2_dual_feed`)

```cpp
#include <ap_fixed.h>
#include <ap_int.h>
#include <stdint.h>

using fp_t = ap_fixed<32, 16>;

enum FsmState : uint8_t {
    STATE_A_IDLE_READY       = 0, // Slow + Fast Oracles green; ready to snipe discounts
    STATE_A_ORDER_IN_FLIGHT  = 1, // Keeper submitted tx; waiting 80 fast ticks (2s) for next Base block
    STATE_A_MAKER_EXIT_LIVE  = 2, // Filled buy; 1-tick Maker exit order resting on Pool 1 (0xc5E5...)
    STATE_B_TOXICITY_DEFENSE = 3, // Fast Hawkes λ_toxic or negative CEX velocity spiked -> block buys
    STATE_C_CIRCUIT_BREAKER  = 4  // Slow Oracle P_jump or depeg_warning -> emergency unwind to USDC
};

struct DualFeedConfig {
    fp_t buy_disparity_ticks;     // e.g. 8.0 ticks below 3B pool_fair_mid
    fp_t hawkes_toxic_thresh_B;   // Fast Oracle gate: max allowed λ_toxic
    fp_t min_cex_velocity_ticks;  // Fast Oracle gate: block buy if CEX dropped > -1.5 ticks in last 500ms
    fp_t p_jump_thresh_C;         // Slow Oracle gate: max allowed Student-t P_jump
    fp_t tend_gas_ticks;          // Base L2 gas cost
};

// Steps 1 Fast Tick (25 ms); every 64/80 ticks (~2,000 ms) processes a Base L2 Block boundary
void step_case_study_2_dual_feed(
    uint32_t  fast_step_idx,      // Increments every 25 ms
    // Fast Feed State (updates every cycle = 25 ms):
    fp_t&     cex_fast_tick,      // 3A north_star_mid (25 ms)
    fp_t&     cex_ema_500ms,      // Fast 500ms (20-tick) CEX EMA to measure short-term CEX velocity
    fp_t&     hawkes_lambda,      // Fast Oracle: Hawkes self-exciting intensity λ_toxic
    // Slow / Block Feed State (updates at Base block boundaries = 2,000 ms):
    fp_t&     slow_basis_ema,     // 3B 30-min structural basis (EURC/USDC - EUR/USD)
    fp_t&     aero_pool_tick,     // On-chain Pool 1 (0xc5E5... ts=1) tick
    fp_t&     p_jump_slow,        // Slow Oracle: Student-t Particle Filter jump probability
    // Strategy & Pending Execution Pipeline (models realistic 2,000 ms L2 block latency):
    FsmState& fsm_state,
    fp_t&     pending_limit_tick, // Limit price of tx currently waiting in Base sequencer queue
    fp_t&     entry_fill_tick,
    fp_t&     maker_exit_tick,
    fp_t&     vault_nav_ticks,
    // Hardware RNG inputs & calibrated constants:
    fp_t z_cex_25ms, fp_t c_burst_25ms, fp_t z_pool_2s, fp_t whale_dump_2s,
    fp_t sigma_cex_25ms, fp_t hawkes_decay_25ms, fp_t hawkes_alpha,
    fp_t kappa_arb_2s, fp_t sigma_pool_2s,
    const fp_t student_t_lut[4096],
    const DualFeedConfig& cfg
) {
#pragma HLS INLINE

    // =========================================================================
    // 1. FAST PATH (Runs EVERY 25 ms cycle): Step CEX & Fast Hawkes Oracle
    // =========================================================================
    cex_fast_tick += (sigma_cex_25ms * z_cex_25ms);
    // 500ms EMA (alpha = 1/20 = 0.05) tracks whether CEX is actively falling right now
    cex_ema_500ms  = cex_ema_500ms + fp_t(0.05) * (cex_fast_tick - cex_ema_500ms);
    fp_t cex_velocity_500ms = cex_fast_tick - cex_ema_500ms;

    // Fast Hawkes Intensity update at 25 ms resolution
    hawkes_lambda  = fp_t(1.0) + (hawkes_decay_25ms * (hawkes_lambda - fp_t(1.0)))
                   + (hawkes_alpha * c_burst_25ms);

    // Fast Defensive Override: if Hawkes spikes or CEX is dropping, immediately lock into STATE_B
    if (fsm_state == STATE_A_IDLE_READY &&
        (hawkes_lambda > cfg.hawkes_toxic_thresh_B || cex_velocity_500ms < cfg.min_cex_velocity_ticks)) {
        fsm_state = STATE_B_TOXICITY_DEFENSE;
    } else if (fsm_state == STATE_B_TOXICITY_DEFENSE &&
               hawkes_lambda <= cfg.hawkes_toxic_thresh_B &&
               cex_velocity_500ms >= cfg.min_cex_velocity_ticks &&
               p_jump_slow < cfg.p_jump_thresh_C) {
        fsm_state = STATE_A_IDLE_READY;
    }

    // =========================================================================
    // 2. BASE L2 BLOCK BOUNDARY (Runs only every 64/80th fast tick = ~2,000 ms)
    // =========================================================================
    if ((fast_step_idx & 0x3F) == 0) {
        fp_t fair_pool_target = cex_fast_tick + slow_basis_ema; // 3B pool_fair_mid
        fp_t prev_pool_tick   = aero_pool_tick;

        // Step On-Chain Aerodrome Pool 1 (mean-reverts to 3B target + whale dumps)
        aero_pool_tick = aero_pool_tick
                       + kappa_arb_2s * (fair_pool_target - aero_pool_tick)
                       + (sigma_pool_2s * z_pool_2s)
                       + whale_dump_2s;

        // Update Slow Oracle (Student-t BRAM LUT on 2s block innovation)
        fp_t block_err = aero_pool_tick - prev_pool_tick;
        int lut_idx    = (int)((block_err * block_err) & 0xFFF);
        p_jump_slow    = fp_t(1.0) - student_t_lut[lut_idx];

        // HARD CIRCUIT BREAKER (State C): Slow Oracle detects structural peg break
        if (p_jump_slow >= cfg.p_jump_thresh_C) {
            if (fsm_state == STATE_A_MAKER_EXIT_LIVE) {
                // Emergency unwind of resting Maker order at current pool tick (-1 bp taker fee)
                vault_nav_ticks += (aero_pool_tick - fp_t(1.0) - entry_fill_tick) - cfg.tend_gas_ticks;
            }
            fsm_state = STATE_C_CIRCUIT_BREAKER;
            return;
        }

        // STEP A: Settle any Keeper Transaction submitted 1 Block (2,000 ms) ago
        if (fsm_state == STATE_A_ORDER_IN_FLIGHT) {
            // 24k-gas slot0() Pre-Revert Guard: only fills if pool is STILL <= pending_limit_tick
            if (aero_pool_tick <= pending_limit_tick) {
                entry_fill_tick = aero_pool_tick + fp_t(1.0); // Paid 1 bp entry fee on Pool 1
                maker_exit_tick = fair_pool_target;           // 1-tick Maker range at [M_t - 1, M_t]
                vault_nav_ticks -= cfg.tend_gas_ticks;
                fsm_state       = STATE_A_MAKER_EXIT_LIVE;
            } else {
                // Competing MEV bot closed the gap during the 2s block delay; 24k-gas guard exited cleanly
                fsm_state       = STATE_A_IDLE_READY;
            }
        }
        // STEP B: Check if resting 1-Tick Maker Exit Order got filled by arbitrageurs
        else if (fsm_state == STATE_A_MAKER_EXIT_LIVE) {
            if (aero_pool_tick >= maker_exit_tick) {
                // Filled as Maker! Earned +1 bp Maker rebate on exit -> 0.0 bps net fee drag
                fp_t net_exit   = maker_exit_tick + fp_t(1.0);
                vault_nav_ticks += (net_exit - entry_fill_tick) - cfg.tend_gas_ticks;
                fsm_state       = STATE_A_IDLE_READY;
            }
        }
        // STEP C: If in STATE_A_IDLE_READY, check if we should submit a Buy Tx for Block B+1
        else if (fsm_state == STATE_A_IDLE_READY) {
            fp_t disparity = fair_pool_target - aero_pool_tick;
            if (disparity >= cfg.buy_disparity_ticks) {
                // Submit tx to Base sequencer (executes 80 fast ticks later at next block!)
                pending_limit_tick = aero_pool_tick + fp_t(1.0);
                fsm_state          = STATE_A_ORDER_IN_FLIGHT;
            }
        }
    }
}
```

---

## 4. What Case Study 2 Reveals That Single-Rate Simulation Missed

When the **80:1 Fast-to-Slow Clock Ratio** and the **1-Block ($2,000\text{ ms}$) Order-In-Flight Delay (`STATE_A_ORDER_IN_FLIGHT`)** are enabled, the FPGA exposes two critical real-world failure modes and how the Dual-Feed FSM solves them:

*(Illustrative Multi-Rate Lab Comparison with Realistic $2,000\text{ ms}$ Base Block Execution Delay):*

| Strategy Variant in Multi-Rate Simulator | Behavior During the 80 CEX Ticks ($2\text{s}$) While Order Is In-Flight | Win Rate | Max `PPS` Drawdown | Net APY |
| :--- | :--- | :--- | :--- | :--- |
| **1. Slow-Only Bot** (Ignores $25\text{ ms}$ Fast CEX Velocity & Hawkes $\lambda_{\text{toxic}}$) | Sees a $7\text{-tick}$ discount at Block $B$, submits `tend()`, but during the $2,000\text{ ms}$ wait for Block $B+1$, Binance drops $-12\text{ ticks}$. Bot gets filled at the start of a macro dump. | $61\%$ | $-3.9\%$ | $+6.8\%$ |
| **2. Dual-Feed FSM Bot (Without `slot0()` Guard)** | Avoids CEX dumps via Fast Hawkes $\lambda_{\text{toxic}}$, but when a competing MEV bot closes the gap during the $2\text{s}$ wait, the transaction reverts and burns $250\text{k}$ gas. | $79\%$ | $-0.9\%$ | $+13.2\%$ |
| **3. Full Dual-Feed FSM + $25\text{ ms}$ CEX Velocity Gate + `slot0()` Guard** | **1.** Fast $25\text{ ms}$ CEX velocity (`cex_velocity_500ms >= -1.5`) blocks entry if CEX is starting to slip.<br>**2.** If an MEV bot beats us during the $2\text{s}$ wait, `slot0()` guard exits cleanly. | **$88\%$** | **$-0.38\%$** | **$+18.9\%$** |

### Two Core Engineering Takeaways from Case Study 2:
1. **Why the $25\text{ ms}$ Fast Feed (`cex_velocity_500ms` & `hawkes_lambda`) Is Mandatory Even on a $2\text{s}$ Blockchain:**
   Even though Base only produces blocks every $2,000\text{ ms}$, sampling CEX every **$25\text{ ms}$** measures the **first derivative (velocity)** of off-chain `EUR/USD` over the last $500\text{ ms}$ (`cex_velocity_500ms`) and order-flow contagion (`hawkes_lambda`). If CEX has started slipping in the last $500\text{ ms}$, transitioning immediately to **`STATE_B_TOXICITY_DEFENSE`** prevents submitting a Base transaction that would land 2 seconds later into a falling market.
2. **Why the Slow Feed (`30m slow_basis_ema` + `evml:events` `safe` & `revert`) Anchors Fair Value:**
   On-chain `EURC/USDC` trades at a slow structural basis (`basis_ema_bps`, e.g., $+2.5\text{ bps}$ premium when on-chain Euro demand is elevated) relative to fiat `EUR/USD`. Computing the entry disparity against the **Slow-adjusted `3B pool_fair_mid` (`cex_fast_tick + slow_basis_ema`)** rather than raw `3A` prevents false triggers caused by normal structural basis offsets.

---

## Annex: Complete `doc/fpga_use_case/` Series

1. [`01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md`](01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md) — Hardware vs. EVM/CEX latency analysis, `RedisSink::emit_fast()` 128-byte binary UDP tap, DeFi hardware firewall, and `Keccak-256` `CREATE2` miner.
2. [`02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md`](02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md) — Logarithmic tick-space derivation, AMD `Vitis Quantitative Finance Library` (`xf_fintech`) setup, and 32-lane `[t_L, t_U]` LP range evaluator.
3. [`03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md`](03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md) — DGN (`EURC/USDC`) vs. Pegged (`USDT/USDC`) quantitative foundation, Ornstein-Uhlenbeck + Merton Jump-Diffusion, 1-cycle Markovian Hawkes intensity ($\lambda_{\text{toxic}}$), Student-$t$ BRAM LUT Particle Filter ($P_{\text{jump}}$), and the 3-State FSM (`State A / B / C`) simulator.
4. [`04_FPGA_Yearn_v3_Aerodrome_Price_Parity_Strategy_Simulator.md`](04_FPGA_Yearn_v3_Aerodrome_Price_Parity_Strategy_Simulator.md) — Yearn v3 (`ERC-4626`) Aerodrome spot limit-buy / CEX parity-sell FSM (`step_yearn_v3_parity_strategy` in `Vitis HLS`), round-trip fee hurdle, moving-target CEX parity rules, and 16-config parallel parameter sweep.
5. [`05_FPGA_to_Yearn_v3_Smart_Contract_Experiment_Lab_Workflow.md`](05_FPGA_to_Yearn_v3_Smart_Contract_Experiment_Lab_Workflow.md) — 6-stage operational workflow from Parquet parameter calibration and Vitis HLS `csim` to FPGA wind-tunnel sweeps, Yearn v3 Solidity contract architecture, and Foundry mainnet fork verification.
6. [`06_Industry_Mastery_and_Hidden_Blind_Spots_Guide.md`](06_Industry_Mastery_and_Hidden_Blind_Spots_Guide.md) — The 6 production blind spots separating independent DeFi builders from institutional HFT/quant desks (LVR & post-trade markouts, Base PGA priority bidding & `slot0()` pre-revert guards, walk-forward anti-overfitting, delta-neutral cross-venue legging, FPGA timing closure `WNS >= 0` & `ULP` drift verification, and Yearn v3 `profitMaxUnlockTime` security).
7. [`07_Case_Study_1_EURC_USDC_Dual_Pool_Parity_Strategy.md`](07_Case_Study_1_EURC_USDC_Dual_Pool_Parity_Strategy.md) — Case Study 1 applying the 6-stage lab to Base `EURC/USDC` (`0xc5E5...` `ts=1` vs. `0xE846...` `ts=50`), discovering the Zero-Net-Fee Hybrid Taker-Entry ($-1\text{ bp}$) / 1-Tick Maker-Exit ($+1\text{ bp}$) Yearn v3 strategy.
8. [`08_Case_Study_2_Dual_Feed_Fast_Slow_Multi_Rate_FSM_Strategy.md`](08_Case_Study_2_Dual_Feed_Fast_Slow_Multi_Rate_FSM_Strategy.md) — *(This document)* Case Study 2 implementing the realistic Dual-Feed (`25 ms` Fast CEX/Hawkes + `2s/22s` Slow Base/Basis) multiplexer, 80:1 multi-rate FPGA clock stepping, and 1-block (`2,000 ms`) in-flight order execution delay.
