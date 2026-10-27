# 02: Dynamic_Discovery.md, verified on-chain (Base, 2026-10-05)

Method: `cast keccak` for topic hashes. `cast call` / `eth_getLogs` against Base mainnet via the public RPC and Infura. Scripts are in `scratch/`. Every number below was **measured**, not recalled.

## A. Factual errors in the doc

| # | Doc says | Verified reality | Impact if used as-is |
|---|---|---|---|
| E1 | EURC = `0x60a3E35Cc655a0052C55d18AEfe34E4ebc1b0DdB` | **No contract code at that address.** The real EURC on Base is `0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42` (`symbol()` = "EURC") | `getPool` returns 0x0: nothing gets tracked |
| E2 | Swap topic0 `0xc42079f9…5b93fc77` | `Swap(address,address,int256,int256,uint160,uint128,int24)` = `0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67`. Confirmed: 2,132 live logs | Zero Swap events received |
| E3 | Deposit topic0 `0x90890809…` | `Deposit(address,uint256,uint128)` = `0x1c8ab8c7f45390d58f58f1d655213a82cca5d12179761a87c16f098813b8f211`. Confirmed on the gauge | Silent miss |
| E4 | Withdraw topic0 `0x884edad9…fb297` | `Withdraw(address,uint256,uint128)` = `0x8903a5b5d08a841e7f68438387f1da20c84dea756379ed37e633ff3854b99b84`. Confirmed. (The doc's prefix belongs to `Withdraw(address,uint256)`) | Silent miss |
| E5 | ClaimRewards topic0 `0x2e086ab2…` | `ClaimRewards(address,uint256)` = `0x1f89f96333d3133000ee447473151fa9606543368f02271c9d95ae14f13bcc67`. Confirmed | Silent miss |
| E6 | NotifyReward = 4-arg `(address,address,uint256,uint256)` on the **gauge** | The 4-arg form is the *VotingReward* (bribe/fee) contract event. The CLGauge form is `NotifyReward(address,uint256)` = `0x095667752957714306e1a6ad83495404412df6fdb932fca6dc849a7ee910d4c1`. **Confirmed**: 2 events in the 6 h after the 2026-10-01 00:00 UTC epoch flip | Silent miss |
| E7 | Mint/Burn topic0 | ✅ correct (`0x7a53080b…`, `0x0c396cd9…`) | — |
| E8 | "EURC/USDC CL Pool (`tickSpacing = 1`)" is the target | ts=1 pool exists, but its **gauge is killed** (`Voter.isAlive = false`, 0 gauge events in 6 h). The **live** gauge is on **ts=50** | Tracking a dead gauge |
| E9 | Pool/gauge events list | Pool is missing `Collect` (`0x70935338…`, 105 events / 6 h). Gauge is missing `ClaimFees(address,uint256,uint256)` = `0xbc567d6cbad26368064baa0ab5a757be46aae4d70f707f9203d9d9b6c8ccbfa3` (**confirmed**, emitted at the epoch flip) | Incomplete LP / fee accounting |
| E10 | Positioning as "Hot-Path Price Updates / sub-millisecond / zero-alloc" | Conflicts with `finality=finalized` (measured lag **539 blocks ≈ 18 min**). Details in master §W3 | Wrong expectations for Bot B |

**Control:** the config validator computes topic0 from the signature and checks `expect_topic0` when given. E2–E6 would have been caught at startup. That is the reason to keep **signatures** in config, not raw hashes.

## B. Resolved EURC/USDC Slipstream contracts (via `CLFactory.getPool`, `Voter.gauges`)

| tickSpacing | Pool | Gauge | Gauge alive | Logs in 6 h (pool) |
|---|---|---|---|---|
| 1 | `0xc5E51044eB7318950B1aFb044FccFb25782C48c1` | `0x85af8D930cB738954d307D6E62F04dd05D839C37` | ❌ killed | 1,021 (Swap 829, Burn 77, Collect 77, Mint 38) |
| 10 | `0x2E63586D8e385ABA0800b061FDD0eB65d23b87E7` | none | — | liquidity 0 → ignore |
| 50 | `0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7` | `0x1f6c9d116CE22b51b0BC666f86B038a6c19900B8` | ✅ | 1,372 (Swap 1,303, Burn 30, Collect 28, Mint 11) |
| 2000 | `0x8f20734720D1BBF200489f79269E218439B7366A` | `0xE8992ECb…268f` | ✅ | liquidity ≈ 0 → ignore |

Infra addresses verified: CLFactory `0x5e7BB104…809A` ✅ (NPM.factory() matches), Voter `0x16613524…80A5` ✅, NPM `0x82792268…5b72` ✅ ("Slipstream Position NFT v1").

ts=50 gauge in 6 h: ClaimRewards 21, Deposit 10, Withdraw 9 (≈160/day).

## C. Measured volume: this is **not** "once a day" data
- Pools ts=1 + ts=50 ≈ **2,400 logs / 6 h ≈ 9,500 / day**. Gauge ≈ 160 / day.
- Still tiny for C++ (≈0.1 events/s), so the architecture holds. It does change some numbers:
  - Stream retention `MINID ~ now−48h` → ≈ 20k entries ≈ 15–25 MB in Redis.
  - Dedup keys at TTL 7 d → ≈ 70k keys ≈ 10 MB.
  - Reconcile `getLogs` on Infura: 500-block pages work. 2,000-block pages got **HTTP 429** and Infura intermittently returns `-32603 "service temporarily unavailable"` (both observed today) → Infura `max_getlogs_range = 500`, and the classifier treats `-32603 service temporarily unavailable` as `Retry`.

## D. Dynamic discovery: decision for v1

**Final v1 contract set (confirmed by you, rev 3):**
| Contract | Address | Events | Finality | Stream |
|---|---|---|---|---|
| ts=1 pool (most swap volume) | `0xc5E51044eB7318950B1aFb044FccFb25782C48c1` | Swap, Mint, Burn, Collect | safe | events |
| ts=50 pool (incentivised LP) | `0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7` | Swap, Mint, Burn, Collect | safe | events |
| ts=50 gauge (live) | `0x1f6c9d116CE22b51b0BC666f86B038a6c19900B8` | Deposit, Withdraw, ClaimRewards, NotifyReward, ClaimFees | finalized | events |
| CLFactory | `0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A` | PoolCreated | finalized | registry |
| Voter | `0x16613524e02ad97eDfeF371bC883F2F5d6C480A5` | GaugeCreated, GaugeKilled, GaugeRevived | finalized | registry |

The killed ts=1 gauge (`0x85af…9C37`) is **not** tracked. One `logs` filter covers all five addresses, with a combined load of ≈ 9.7k events/day.
Agree with the doc's static-first recommendation, with one simplification: **no separate discovery process.**
- In v1, `CLFactory.PoolCreated` and `Voter.GaugeCreated / GaugeKilled / GaugeRevived` are **just more contracts in the same static config**. topic0 lists are OR-ed across addresses and these contracts never emit each other's events, so one filter works. Volume is low.
- These events route to a separate stream (`registry`) through a per-contract `stream` override. Downstream and the operator decide whether to add a pool; adding one is a config change plus a redeploy. No auto-subscribe in v1.
- E8 shows why this matters: the ts=1 gauge was killed. `GaugeKilled` / `GaugeCreated` events are exactly what would have told you to switch to ts=50.
- **Signatures to confirm from live logs in Phase 1 fixture capture** (not recalled): `PoolCreated(address,address,int24,address)` = `0xab0d57f0…9ac2`; `GaugeCreated(address,address,address,address,address,address,address,address)` = `0xef9f7d1f…79e1`; `GaugeKilled(address)` = `0x04a5d3f5…2ba7`; `GaugeRevived(address)` = `0xed18e9fa…b1aa`.
- v2 (later): auto-subscribe means adding a second `eth_subscribe` on the same socket and backfilling the new address from its creation block, followed by an atomic checkpoint merge. This is non-trivial and is deferred on purpose.

## E. Finality measured (single sample, 2026-10-05 14:19 UTC)
`head 52,210,318` · `safe 52,210,305` (**−13 blocks ≈ 26 s**) · `finalized 52,209,779` (**−539 blocks ≈ 18 min**).
Second sample (QuickNode, 2026-10-05 15:32 UTC): `head 52,212,496` · `safe −9` (≈ 18 s) · `finalized −625` (≈ 21 min).
Phase 6 will collect a 24 h distribution for the README.
