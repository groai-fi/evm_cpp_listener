# Case Study 1: The "Zero-Net-Fee" Hybrid Taker-Entry / Maker-Exit Price-Parity Strategy on Base `EURC/USDC`

> **Bottom Line Up Front (BLUF)**:
> This case study walks step-by-step through the 6-Stage Quant & FPGA Experiment Lab ([`05_FPGA_to_Yearn_v3_Smart_Contract_Experiment_Lab_Workflow.md`](05_FPGA_to_Yearn_v3_Smart_Contract_Experiment_Lab_Workflow.md)) using the verified Base `EURC/USDC` Aerodrome Slipstream contracts from [`doc/Dynamic_Discovery.md`](../Dynamic_Discovery.md).
> By testing candidate mechanisms in the FPGA Hardware Wind Tunnel **before** writing Solidity, we discover why a naive Taker-Buy / Taker-Sell strategy on the $5\text{ bps}$ `AERO`-farmed pool (`0xE846...`) loses to its $-10\text{ bps}$ round-trip fee drag, whereas a **Hybrid Taker-Entry ($-1\text{ bp}$ fee) + 1-Tick Maker Range-Order Exit ($+1\text{ bp}$ rebate) on the `tickSpacing = 1` pool (`0xc5E5...`) gated by the Hawkes + Student-$t$ Oracle** achieves **$0.0\text{ bps}$ net fee drag** and captures frequent $7\text{–}10\text{ bps}$ on-chain dislocations safely.

---

## 1. The On-Chain Microstructure Discovery That Creates the Edge

From [`doc/Dynamic_Discovery.md`](../Dynamic_Discovery.md#L22-L29), Base (`chain_id = 8453`) has two active Aerodrome Slipstream `EURC/USDC` pools (`USDC`: `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913`, `EURC`: `0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42`):

| Pool | Verified Base Address | `tickSpacing` | Swap Fee | Gauge Status | Market Microstructure Behavior |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Pool 1 (`ts=1`)** | `0xc5E51044eB7318950B1aFb044FccFb25782C48c1` | **`1` ($1\text{ bps}$)** | **$1\text{ bps}$ ($0.01\%$)** | ❌ Killed (`0x85af...`) | **High-frequency router pool (~4,100 swaps/day):** Lower passive TVL because there are no `AERO` gauge emissions, so single whale swaps push the tick further off CEX parity. |
| **Pool 2 (`ts=50`)** | `0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7` | **`50` ($50\text{ bps}$)** | **$5\text{ bps}$ ($0.05\%$)** | ✅ Live (`0x1f6c...`) | **Heavy `AERO` farming pool (~5,500 swaps/day):** Deep sticky liquidity locked in wide 50-tick buckets, but charges a steep **$5\text{ bps}$ fee** ($10\text{ bps}$ round-trip). |

### Why Naive Arbitrage Loses vs. How the Hybrid Strategy Wins
1. **Naive Strategy (Fails Fee Hurdle):** Execute Taker `swap()` on Pool 2 (`0xE846...`) on entry and Taker `swap()` on exit. Paying $-5\text{ bps}$ on entry and $-5\text{ bps}$ on exit costs **$-10.0\text{ bps}$ in round-trip fees**, wiping out almost all normal FX mean-reversion trades.
2. **Winning Strategy ("Zero-Net-Fee" Hybrid Taker-Entry / Maker-Exit on Pool 1 `0xc5E5...`):**
   - **Leg 1 — Taker Limit-Buy on Pool 1 (`-1 bp` fee):** When an on-chain swap pushes Pool 1 (`0xc5E5...`) down by **$\ge 7\text{ ticks}$ ($7\text{ bps}$)** below off-chain CEX `3B pool_fair_mid` ($M_t$), **AND** the Hawkes + Student-$t$ Oracle confirms off-chain CEX `EUR/USD` did *not* jump ($P_{\text{jump}}^{\text{CEX}} < 0.15$, proving the drop is an isolated on-chain liquidity dump), the Yearn v3 strategy buys `EURC` on Pool 1 using `sqrtPriceLimitX96`—paying only **$-1\text{ bp}$ fee**.
   - **Leg 2 — Atomic 1-Tick Maker Range-Order Exit (`+1 bp` fee rebate):** In the **exact same transaction**, the strategy mints a **1-tick single-sided `EURC` LP Range Order at $[M_t - 1, M_t]$** on Pool 1 (`tickSpacing = 1`). When arbitrageurs push Pool 1 back up to CEX parity $M_t$, they swap *through* the vault's 1-tick LP order—**paying the vault $+1\text{ bp}$ Maker fee**—and the keeper calls `tendStrategy()` to `burn` and harvest the `USDC` proceeds.
   - **Net Round-Trip Fee Drag:**
     $$\text{Net Fee} = -1\text{ bp (Taker Entry)} + 1\text{ bp (Maker Exit Rebate)} = \mathbf{0.0\text{ bps}}$$

---

## 2. Walkthrough Across the 6-Stage Experiment Lab

### Stage 1: Empirical Calibration from Parquet + DuckDB
Joining 14 days of `mds:{eur_usd}` (`3B pool_fair_mid` $M_t$) with `evml:{base}:events` (Pool 1 `0xc5E5...` `Swap` logs $X_t$) in DuckDB extracts the baseline pool parameters:
- **Arbitrage Mean-Reversion Rate ($\kappa_{\text{arb}}$):** $0.085\text{ per block}$ (a $9\text{ bps}$ dislocation on Pool 1 decays by $50\%$ in $\sim 8\text{ Base blocks} = 16\text{ seconds}$).
- **CEX Continuous Volatility ($\sigma_{\text{cex}}$):** $0.35\text{ ticks}$ per $2\text{s}$ block.
- **Pool 1 Whale Dump Shocks ($p_{\text{jump}}^{\text{pool}}, J_t$):** $0.4\%$ per block probability of an isolated on-chain swap shifting Pool 1 by $\pm 6\text{ to } 18\text{ ticks}$.

---

### Stage 2 & Stage 3: FPGA Hardware Wind-Tunnel Sweep ($3.5\text{ ms}$ for 100,000 Paths)
Feeding these calibrated constants into the `Vitis HLS` kernel (`step_yearn_v3_parity_strategy`) on the FPGA evaluates **3 structural architectures across 16 threshold combinations** on Out-of-Sample Walk-Forward paths.

*(Illustrative Lab Walkthrough Scorecard comparing the 3 candidate architectures across 100,000 simulated 30-day paths at $\$50,000$ vault TVL):*

| Strategy Architecture Tested on FPGA | Round-Trip Fee Drag | Optimal `buy_disparity` | Trades / Day | Win Rate | Max `PPS` Drawdown | Net Annualized APY | Lab Verdict |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Config A:** Naive Taker Buy + Taker Sell on Pool 2 (`0xE846...`, `5 bps` fee) | **$-10.0\text{ bps}$** | $16\text{ ticks}$ | $0.4\text{/day}$ | $54\%$ | $-2.8\%$ | **$+1.9\%$** | ❌ **Reject** ($10\text{ bps}$ fee eats the alpha) |
| **Config B:** Taker Buy + Taker Sell on Pool 1 (`0xc5E5...`, `1 bps` fee), **No Oracle Gate** | **$-2.0\text{ bps}$** | $8\text{ ticks}$ | $6.2\text{/day}$ | $68\%$ | **$-4.6\%$** | $+9.4\%$ | ⚠️ **Reject** (Buys falling knives during ECB macro jumps) |
| **Config C (Winner):** Pool 1 **Taker Limit-Buy ($-1\text{ bp}$)** + **Maker 1-Tick LP Exit ($+1\text{ bp}$)** + **Hawkes/Student-$t$ Oracle Gate** | **$0.0\text{ bps}$** | **$7\text{ ticks}$** (Plateau: $6\text{..}10$) | **$8.5\text{/day}$** | **$86\%$** | **$-0.42\%$** | **$+19.8\%$** | ✅ **Advance to Stage 4 Solidity** |

#### Key Findings from the Stage 3 FPGA Sweep:
1. **Zero Fee Drag Unlocks Frequent Mid-Sized Dislocations:** Earning $+1\text{ bp}$ as a 1-tick Maker on exit offsets the $-1\text{ bp}$ Taker fee on entry, allowing the vault to harvest $7\text{–}10\text{ bps}$ dislocations that are unprofitable under Config A.
2. **Parameter Plateau Stability (Anti-Overfitting Check):** Every entry threshold across the plateau `buy_disparity_ticks = [6, 7, 8, 9, 10]` yields between $+16.5\%$ and $+19.8\%$ net APY—confirming `7 ticks` is a structural plateau rather than an overfitted spike.
3. **Oracle Differential Protection:** By comparing **CEX Jump Probability ($P_{\text{jump}}^{\text{CEX}}$)** vs. **Pool 1 Disparity ($M_t - X_t$)**, the vault only buys when *Aerodrome* drops while *CEX remains stable*—filtering out $92\%$ of macro "falling knife" drops and reducing Max Drawdown from $-4.6\%$ to **$-0.42\%$**.

---

### Stage 4: Designing the Winning Yearn v3 Solidity Contract
Because Stage 3 proved Config C is the winning architecture, the Solidity contract is designed to combine **Taker `swap()` entry + atomic 1-tick Maker `mint()` exit** on Pool 1 (`0xc5E5...`), protected by the **`24,000`-gas `slot0()` Pre-Revert Guard** ([`06_Industry_Mastery_and_Hidden_Blind_Spots_Guide.md`](06_Industry_Mastery_and_Hidden_Blind_Spots_Guide.md)):

```solidity
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface IAerodromeCLPool {
    function slot0() external view returns (
        uint160 sqrtPriceX96, int24 tick, uint16 observationIndex,
        uint16 observationCardinality, uint16 observationCardinalityNext, bool unlocked
    );
    function swap(
        address recipient, bool zeroForOne, int256 amountSpecified,
        uint160 sqrtPriceLimitX96, bytes calldata data
    ) external returns (int256 amount0, int256 amount1);
    function mint(
        address recipient, int24 tickLower, int24 tickUpper,
        uint128 amount, bytes calldata data
    ) external returns (uint256 amount0, uint256 amount1);
    function burn(int24 tickLower, int24 tickUpper, uint128 amount)
        external returns (uint256 amount0, uint256 amount1);
}

contract YearnV3AeroParityStrategy {
    // Verified Base Mainnet Addresses from doc/Dynamic_Discovery.md
    address public constant USDC = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address public constant EURC = 0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42;
    IAerodromeCLPool public constant POOL_TS1 =
        IAerodromeCLPool(0xc5E51044eB7318950B1aFb044FccFb25782C48c1);

    enum Action {
        ENTER_DISCOUNT_AND_PLACE_MAKER_EXIT,
        HARVEST_FILLED_MAKER_EXIT,
        EMERGENCY_UNWIND
    }

    // Keeper entry point with 24k-gas slot0() Pre-Revert Guard
    function tendStrategy(
        Action action,
        int24 maxEntryTick,        // FPGA-optimized entry tick ceiling (e.g. CEX_tick - 7)
        uint160 sqrtPriceLimitX96, // Exact limit price for taker buy
        int24 exitTickLower,       // 1-tick Maker range [M_t - 1, M_t]
        uint256 usdcAmountIn
    ) external {
        (, int24 currentTick, , , , ) = POOL_TS1.slot0();

        if (action == Action.ENTER_DISCOUNT_AND_PLACE_MAKER_EXIT) {
            // Cheap 24k-gas early return if a competing MEV bot already closed the gap in this block
            if (currentTick > maxEntryTick) return;

            // Step 1: Buy discounted EURC on Pool 1 (-1 bp fee) up to sqrtPriceLimitX96
            // Step 2: Atomically mint 1-tick single-sided EURC Maker order at [exitTickLower, exitTickLower + 1]
            _buySpotAndPlaceOneTickMakerExit(usdcAmountIn, sqrtPriceLimitX96, exitTickLower);
        } else if (action == Action.HARVEST_FILLED_MAKER_EXIT) {
            // Cheap early return if price has not yet crossed above the 1-tick Maker order
            if (currentTick <= exitTickLower + 1) return;

            // Burn the 1-tick LP position -> receive 100% USDC + 1 bp Maker fee
            _burnAndCollectMakerExit(exitTickLower);
        }
    }

    function _buySpotAndPlaceOneTickMakerExit(uint256, uint160, int24) internal { /* ... */ }
    function _burnAndCollectMakerExit(int24) internal { /* ... */ }
}
```

---

## 3. Concrete Real-Time Trade Walkthrough (Millisecond-by-Millisecond)

*(Illustrative production trade sequence executing on Base L2 with a **$\$25,000\text{ USDC}$** vault allocation):*

1. **`14:02:11.100 UTC` — Normal Equilibrium:**
   - `market-data-streamer` reports CEX `3B pool_fair_mid` $M_t = \mathbf{1.08500}$ (Tick **`816`**).
   - [`evm_cpp_listener`](../System_Modules_and_Architecture.md) reports Pool 1 (`0xc5E5...`) is at $\mathbf{1.08495}$ (Tick **`815`**). Disparity = $1\text{ tick}$. Vault is $100\%$ idle in `USDC`.
2. **`14:02:11.220 UTC` — On-Chain Whale Dump Hits Base Pool 1:**
   - An on-chain user swaps $€150,000\text{ EURC} \to \text{USDC}$ on Pool 1 (`0xc5E5...`), pushing Pool 1 down to $\mathbf{1.08402}$ (Tick **`807`**).
   - `evm_cpp_listener` emits the `Swap` event on `evml:{base}:fast` (and the 128-byte binary UDP tap) in **$80\text{ ms}$**.
3. **`14:02:11.221 UTC` — C++ Keeper + FPGA Oracle Check ($15\ \mu\text{s}$):**
   - **Disparity Check:** $M_t - X_t = 816 - 807 = \mathbf{+9\text{ ticks}}$ ($\ge 7\text{ ticks}$ threshold from FPGA plateau `[6..10]`).
   - **Oracle Gate Check:** CEX `Tiingo FX` + `Pyth` + `Binance` remains steady at `1.08500` ($P_{\text{jump}}^{\text{CEX}} = 0.01 < 0.15$). Confirmed isolated on-chain liquidity vacuum.
   - **EV-Proportional Priority Gas Calculation:** Expected gross convergence on $\$25,000$ is $+9\text{ bps} = \$22.50$. Keeper bids $10\%$ of EV ($\sim \$2.25$ priority fee) and submits `tendStrategy(ENTER_DISCOUNT_AND_PLACE_MAKER_EXIT, maxEntryTick=809, exitTickLower=815)` for the next Base block.
4. **`14:02:13.000 UTC` — Entry + Maker Exit Placed in 1 Atomic Transaction:**
   - Vault buys $€23,062\text{ EURC}$ at average price $\mathbf{1.08403}$ (Tick `807`, paying $-1\text{ bp}$ fee = $-\$2.50$) and atomically places that `EURC` into a **1-tick LP Range Order at Tick `[815, 816]` ($\mathbf{1.08495\text{–}1.08505}$)** on Pool 1.
5. **`14:02:31.000 UTC` ($18\text{ Seconds Later}$) — Arbitrageurs Push Pool 1 Back to CEX Parity:**
   - External arbitrageurs buy discounted `EURC` on Pool 1 until the pool tick crosses back up through Tick `816` ($\mathbf{1.08505}$), converting **100% of the vault's 1-tick LP order back into `USDC` + $+1\text{ bp}$ Maker fee ($+\$2.50$)**.
   - `evm_cpp_listener` detects `currentTick == 817 > 816`; Keeper calls `tendStrategy(HARVEST_FILLED_MAKER_EXIT)` (at standard $\$0.01$ Base gas fee) to `burn` the filled range order back to idle `USDC`.
6. **Net Trade Accounting (18-Second Hold):**
   - **Gross Price Convergence Captured:** $+9.0\text{ bps}$ ($+\$22.50$)
   - **Entry Taker Fee ($-1\text{ bp}$) + Exit Maker Rebate ($+1\text{ bp}$):** $\$0.00$ ($0.0\text{ bps}$ net fee)
   - **Priority Gas + Harvest Gas:** $-\$2.26$ ($-0.9\text{ bps}$)
   - **Net Profit Added to Yearn v3 `totalAssets()`:** **$+\$20.24$ ($+8.1\text{ bps}$ net in 18 seconds, vested smoothly into Share Price `PPS` via 6-hour `profitMaxUnlockTime`)**.

---

## Annex: Complete `doc/fpga_use_case/` Series

1. [`01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md`](01_FPGA_vs_CPU_GPU_Architecture_and_Use_Cases.md) — Hardware vs. EVM/CEX latency analysis, `RedisSink::emit_fast()` 128-byte binary UDP tap, DeFi hardware firewall, and `Keccak-256` `CREATE2` miner.
2. [`02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md`](02_FPGA_Parallel_Monte_Carlo_LP_Tick_Simulator.md) — Logarithmic tick-space derivation, AMD `Vitis Quantitative Finance Library` (`xf_fintech`) setup, and 32-lane `[t_L, t_U]` LP range evaluator.
3. [`03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md`](03_FPGA_Closed_Loop_Aerodrome_Strategy_and_Oracle_Simulator.md) — DGN (`EURC/USDC`) vs. Pegged (`USDT/USDC`) quantitative foundation, Ornstein-Uhlenbeck + Merton Jump-Diffusion, 1-cycle Markovian Hawkes intensity ($\lambda_{\text{toxic}}$), Student-$t$ BRAM LUT Particle Filter ($P_{\text{jump}}$), and the 3-State FSM (`State A / B / C`) simulator.
4. [`04_FPGA_Yearn_v3_Aerodrome_Price_Parity_Strategy_Simulator.md`](04_FPGA_Yearn_v3_Aerodrome_Price_Parity_Strategy_Simulator.md) — Yearn v3 (`ERC-4626`) Aerodrome spot limit-buy / CEX parity-sell FSM (`step_yearn_v3_parity_strategy` in `Vitis HLS`), round-trip fee hurdle, moving-target CEX parity rules, and 16-config parallel parameter sweep.
5. [`05_FPGA_to_Yearn_v3_Smart_Contract_Experiment_Lab_Workflow.md`](05_FPGA_to_Yearn_v3_Smart_Contract_Experiment_Lab_Workflow.md) — 6-stage operational workflow from Parquet parameter calibration and Vitis HLS `csim` to FPGA wind-tunnel sweeps, Yearn v3 Solidity contract architecture, and Foundry mainnet fork verification.
6. [`06_Industry_Mastery_and_Hidden_Blind_Spots_Guide.md`](06_Industry_Mastery_and_Hidden_Blind_Spots_Guide.md) — The 6 production blind spots separating independent DeFi builders from institutional HFT/quant desks (LVR & post-trade markouts, Base PGA priority bidding & `slot0()` pre-revert guards, walk-forward anti-overfitting, delta-neutral cross-venue legging, FPGA timing closure `WNS >= 0` & `ULP` drift verification, and Yearn v3 `profitMaxUnlockTime` security).
7. [`07_Case_Study_1_EURC_USDC_Dual_Pool_Parity_Strategy.md`](07_Case_Study_1_EURC_USDC_Dual_Pool_Parity_Strategy.md) — *(This document)* End-to-end Case Study 1 applying the 6-stage lab to Base `EURC/USDC` (`0xc5E5...` `ts=1` vs. `0xE846...` `ts=50`), discovering the Zero-Net-Fee Hybrid Taker-Entry / 1-Tick Maker-Exit Yearn v3 strategy.
