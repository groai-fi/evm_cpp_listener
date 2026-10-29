# EVM C++ Listener: Aerodrome Slipstream EURC/USDC & Dynamic Discovery Specification

Here is the verified specification for tracking **EURC/USDC** on Base (`chainId: 8453`) on Aerodrome Slipstream, along with measured on-chain event signatures, topic hashes, and the architectural design for **Static Sharding & Discovery Streams**.

---

### 1. Verified Contract Addresses on Base (`chainId: 8453`)

#### Core Tokens
| Token | Address | Decimals | Verification Note |
| :--- | :--- | :--- | :--- |
| **USDC** | `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913` | 6 | Native Circle USDC |
| **EURC** | `0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42` | 6 | Native Circle EURC (`symbol() == "EURC"`) |

#### Aerodrome Slipstream Infrastructure
| Contract | Address | Purpose |
| :--- | :--- | :--- |
| **CLFactory** | `0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A` | Concentrated liquidity pool deployer |
| **Voter** | `0x16613524e02ad97eDfeF371bC883F2F5d6C480A5` | Gauge creation, voting & reward distribution |
| **NonfungiblePositionManager (NPM)** | `0x827922686190790b37229fd06084350E74485b72` | ERC-721 LP position manager ("Slipstream Position NFT v1") |

#### Target Pools & Gauges Status (Measured On-Chain)
| tickSpacing | Pool Address | Gauge Address | Gauge Status | Daily Volume (est.) | Tracking Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **ts = 1** (0.01%) | `0xc5E51044eB7318950B1aFb044FccFb25782C48c1` | `0x85af8D930cB738954d307D6E62F04dd05D839C37` | ❌ Killed (`isAlive=false`) | ~4,100 logs/day (Swap heavy) | **Track Pool Only** (`finality=safe`) |
| **ts = 50** (0.05%) | `0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7` | `0x1f6c9d116CE22b51b0BC666f86B038a6c19900B8` | ✅ Live | ~5,500 logs/day (Pool) + ~160/day (Gauge) | **Track Pool (`safe`) & Gauge (`finalized`)** |
| **ts = 10** | `0x2E63586D8e385ABA0800b061FDD0eB65d23b87E7` | None | — | Liquidity ≈ 0 | Ignored |
| **ts = 2000** | `0x8f20734720D1BBF200489f79269E218439B7366A` | `0xE8992ECb…268f` | ✅ | Liquidity ≈ 0 | Ignored |

---

### 2. Event Signatures, Verified Topic0 Hashes & ABI Layouts

> [!IMPORTANT]
> Always configure human-readable **canonical signatures** in configuration files and derive topic0 at startup using `keccak256`. Use `expect_topic0` for assertions to prevent configuration errors.

#### A. Slipstream Pool Events (`finality: safe`, Stream: `evml:{shard}:events`)

##### 1. `Swap` (Price & Volume Updates)
```solidity
event Swap(
    address indexed sender,
    address indexed recipient,
    int256 amount0,
    int256 amount1,
    uint160 sqrtPriceX96,
    uint128 liquidity,
    int24 tick
);
```
- **Topic0**: `0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67`
- **Topic1 (indexed)**: `sender` (padded to 32 bytes)
- **Topic2 (indexed)**: `recipient` (padded to 32 bytes)
- **Data payload (160 bytes total)**:
  - `[0x00 .. 0x20]`: `amount0` (`int256`)
  - `[0x20 .. 0x40]`: `amount1` (`int256`)
  - `[0x40 .. 0x60]`: `sqrtPriceX96` (`uint160` padded to `uint256`)
  - `[0x60 .. 0x80]`: `liquidity` (`uint128` padded to `uint256`)
  - `[0x80 .. 0xa0]`: `tick` (`int24` sign-extended to `int256`)

##### 2. `Mint` & `Burn` (Liquidity Depth Tracking)
- **`Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)`**:
  - **Topic0**: `0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde`
- **`Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)`**:
  - **Topic0**: `0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c`

##### 3. `Collect` (Fee & Position Settlement)
```solidity
event Collect(
    address indexed owner,
    address recipient,
    int24 indexed tickLower,
    int24 indexed tickUpper,
    uint128 amount0,
    uint128 amount1
);
```
- **Topic0**: `0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0`

---

#### B. Aerodrome CLGauge Events (`finality: finalized`, Stream: `evml:{shard}:events`)

##### 1. `Deposit`
```solidity
event Deposit(
    address indexed user,
    uint256 indexed tokenId,
    uint128 indexed liquidityToStake
);
```
- **Topic0**: `0x1c8ab8c7f45390d58f58f1d655213a82cca5d12179761a87c16f098813b8f211`

##### 2. `Withdraw`
```solidity
event Withdraw(
    address indexed user,
    uint256 indexed tokenId,
    uint128 indexed liquidityToWithdraw
);
```
- **Topic0**: `0x8903a5b5d08a841e7f68438387f1da20c84dea756379ed37e633ff3854b99b84`

##### 3. `ClaimRewards`, `NotifyReward` & `ClaimFees`
- **`ClaimRewards(address indexed from, uint256 amount)`**:
  - **Topic0**: `0x1f89f96333d3133000ee447473151fa9606543368f02271c9d95ae14f13bcc67`
- **`NotifyReward(address indexed from, uint256 amount)`** *(CLGauge standard 2-arg event)*:
  - **Topic0**: `0x095667752957714306e1a6ad83495404412df6fdb932fca6dc849a7ee910d4c1`
- **`ClaimFees(address indexed from, uint256 claimed0, uint256 claimed1)`**:
  - **Topic0**: `0xbc567d6cbad26368064baa0ab5a757be46aae4d70f707f9203d9d9b6c8ccbfa3`

---

#### C. Registry & Governance Events (`finality: finalized`, Stream: `evml:{shard}:registry`)

- **`PoolCreated(address indexed token0, address indexed token1, int24 indexed tickSpacing, address pool)`**:
  - **Topic0**: `0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2` *(live-confirmed on Base mainnet at block `17827297`, tx `0x29ed1118dd6980112a18d16f2f6238656a37fbf600776476b61946fb1bbf4218`)*
- **`GaugeCreated(address indexed poolFactory, address indexed votingRewardsFactory, address indexed gaugeFactory, address pool, address bribeVotingReward, address feeVotingReward, address gauge, address creator)`**:
  - **Canonical ABI signature**: `GaugeCreated(address,address,address,address,address,address,address,address)`
  - **Topic0**: `0xef9f7d1ffff3b249c6b9bf2528499e935f7d96bb6d6ec4e7da504d1d3c6279e1` *(live-confirmed on Base mainnet at block `17827325`, tx `0x64c6d063058184b69d0a4a43e90beb619b227b49750180822fcd65e17fccbf2a`)*
- **`GaugeKilled(address indexed gauge)`**:
  - **Topic0**: `0x04a5d3f5d80d22d9345acc80618f4a4e7e663cf9e1aed23b57d975acec002ba7`
- **`GaugeRevived(address indexed gauge)`**:
  - **Topic0**: `0xed18e9faa3dccfd8aa45f69c4de40546b2ca9cccc4538a2323531656516db1aa`

---

### 3. Finality Latency vs. Execution Architecture

On Base (`chainId: 8453`), finality tags exhibit measured latencies:
- **`safe` finality tag**: ~13 blocks behind head (~26 seconds).
- **`finalized` finality tag**: ~539 blocks behind head (~18 minutes).

The listener is an **asynchronous event ingestion and indexing pipeline** that feeds Redis Streams. Trading bots and yield aggregators consume from the Redis stream:
- **High-throughput pool data** (Swap, Mint, Burn, Collect) operates at `safe` finality tag with automatic `revert` handling if safe-reorg occurs.
- **Settlement & accounting data** (Gauge staking, rewards, registry events) operates at `finalized` finality tag for zero-reorg certainty.

---

### 4. Dynamic Discovery Decision for v1

In v1, dynamic discovery is integrated into the **single static configuration shard** without requiring a complex multi-process discovery daemon:
1. **Unified Filter**: `CLFactory` and `Voter` are added as contracts in `config/base.toml`.
2. **Stream Partitioning**: Pool and gauge events route to `evml:{base_eurc_usdc}:events`, while `PoolCreated` / `GaugeCreated` / `GaugeKilled` / `GaugeRevived` route to `evml:{base_eurc_usdc}:registry`.
3. **Operator Notification**: Downstream consumers or n8n monitor the `registry` stream. When a new pool or gauge is deployed, operators update the TOML config and redeploy.
4. **v2 Scope**: Fully automated runtime `eth_subscribe` expansion and sub-checkpoint backfill are deferred to v2.