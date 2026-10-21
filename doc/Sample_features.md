# EVM C++ Listener Architecture: Core Components & Best Practices

This document specifies the architectural patterns, state machines, and resilience mechanisms implemented in the **EVM C++ Log Listener** (`evm_cpp_listener`), addressing key edge cases in asynchronous blockchain event streaming.

---

## 1. Architectural Overview & Component Responsibilities

```
                                  ┌────────────────────────┐
                                  │ QuickNode / Infura WSS │
                                  └───────────┬────────────┘
                                              │ newHeads + logs (filtered)
                                              ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Session (Boost.Beast / Asio WebSocket)                                                 │
│ • Full-jitter exponential backoff (250ms -> 30s)                                       │
│ • Monotonically increasing JSON-RPC Request IDs (pending map)                          │
│ • Dynamic AIMD getLogs Range Adaptation (Additive Increase / Multiplicative Decrease)  │
│ • Padded Buffer (simdjson::SIMDJSON_PADDING)                                           │
└─────────────────────────────────────────────┬──────────────────────────────────────────┘
                                              │ Live Logs (buffered during backfill)
                                              ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Pipeline & Aggregator                                                                  │
│ • State Machine: Connecting -> Subscribing (Buffer) -> Backfill (Gate) -> Drain -> Live│
│ • Finality Gates: G_s (safe tag ~26s) & G_f (finalized tag ~18m)                      │
│ • Checkpoint Advancement: Driven by Gate Sweeps (NOT raw log frequency)                │
│ • Canonical Hash Ring (512 blocks) & Reorg Diff Detector -> Revert Event Emitter       │
│ • In-Memory LRU Cache                                                                  │
└─────────────────────────────────────────────┬──────────────────────────────────────────┘
                                              │ Deduplicated Events (log / revert)
                                              ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Sink Engine (redis-plus-plus & Local WAL)                                              │
│ • Redis Lua Atomic Dedup: SET NX EX 7d + XADD MINID~48h                                │
│ • Dedup Key format: evml:{shard}:dedup:{blockHash}:{txHash}:{logIndex}:{type}          │
│ • Fallback Bounded Queue -> CRC32-Checked Disk WAL with Rotation & Backpressure        │
│ • Active-Passive Lease: evml:{shard}:leader (TTL 20s)                                  │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. State Machine & Transition Invariants

```
INIT ──► CONNECTING ──► SUBSCRIBING (Buffer) ──► BACKFILL (to Gate) ──► DRAIN ──► LIVE
            ▲                   │                       │                 │         │
            └───────────────────┴───────────────────────┴─────────────────┴─────────┘
                       Error / Stall / Ping Timeout ──► Full-Jitter Backoff
```

### Critical Invariant: Zero-Gap Ordering
1. **Subscribe First, Buffer Live**: On WebSocket connect, `newHeads` and `logs` subscriptions are established immediately. All incoming live events are pushed into `pending_buffer`.
2. **Backfill Up to Provider Gate**: The backfill cursor reads the checkpoint and queries the getLogs provider's **own finality tags** ($G_s$ and $G_f$). It backfills only up to the gate, ensuring no unconfirmed or reorg-vulnerable blocks are committed during sync.
3. **Drain & Sort**: Before entering `LIVE` state, the `pending_buffer` is sorted by `(blockNumber, logIndex)` and drained into the pipeline.

---

## 3. Resilience & Edge Case Solutions

### A. Gate-Driven Checkpointing (Fix for S1)
* **Problem**: Advancing checkpoints on `newHeads` causes unconfirmed blocks to be marked as processed. Advancing only "per log" causes checkpoint starvation on sparse contracts.
* **Solution**: Checkpoints (`evml:{shard}:ckpt` hash with fields `safe` and `finalized`) advance **when a finality gate sweep completes successfully**, independent of whether any logs were emitted.

### B. Shared Redis Lua Atomic Deduplication (Fix for S2)
* **Problem**: In-memory dedup is lost on restart, causing duplicate deliveries after failover.
* **Solution**: Deduplication is executed inside Redis via an atomic Lua script:
  ```lua
  -- KEYS[1]: dedup key, KEYS[2]: stream key
  -- ARGV[1]: dedup TTL (seconds), ARGV[2...]: XADD arguments
  if redis.call("SET", KEYS[1], "1", "NX", "EX", ARGV[1]) then
      return redis.call("XADD", KEYS[2], unpack(ARGV, 2))
  else
      return nil
  end
  ```
* **Dedup Key**: `evml:{shard}:dedup:{blockHash}:{txHash}:{logIndex}:{type}` (TTL: 7 days).
* The `:type` suffix (`:log` vs `:revert`) ensures that `revert` events for reorged logs are never dropped by `SET NX`.

### C. Watchdogs & Backfill Progress (Fix for S3)
* **Problem**: A fixed 120s total backfill timeout triggers infinite crash loops after long downtime.
* **Solution**:
  - **Per-Request Timeout**: 10s per `eth_getLogs` RPC call.
  - **Progress Watchdog**: The watchdog timer resets every time a valid page is processed and the backfill cursor advances.

### D. Dynamic AIMD Range Sizing & Request Dispatching (Fix for S4)
* **Problem**: Fixed range halving never recovers, and static JSON-RPC IDs break with concurrent requests.
* **Solution**:
  - **AIMD Range Controller**: On RPC error/429/timeout, reduce range (`range = max(min_range, range / 2)`). On consecutive successes, incrementally expand range (`range = min(max_range, range + step)`).
  - **Monotonic Request IDs**: Every RPC request generates an incrementing `uint64_t id` mapped to a callback in a pending request table.

### E. Redis Sink & Framed WAL Fallback (Fix for S5)
* **Problem**: Synchronous HTTP POST webhooks create head-of-line blocking and unbounded disk usage.
* **Solution**:
  - **Primary Sink**: Asynchronous Redis Streams pipeline (`XADD` with `MINID ~ 48h`).
  - **Secondary Fallback**: Bounded in-memory queue (10k items).
  - **Disk WAL**: CRC32-framed append-only log on `/data` with 512 MB maximum size cap.
  - **Backpressure**: When the WAL reaches maximum capacity, WebSocket frame reads are paused, prioritizing system correctness and clean restart over data loss.

### F. Verified Event Signatures & Topic0 Hashes (Fix for S6)
Event signatures on Base (`chainId: 8453`):
* **`Deposit(address,uint256,uint128)`**: `0x1c8ab8c7f45390d58f58f1d655213a82cca5d12179761a87c16f098813b8f211`
* **`Withdraw(address,uint256,uint128)`**: `0x8903a5b5d08a841e7f68438387f1da20c84dea756379ed37e633ff3854b99b84`
* **`Swap(address,address,int256,int256,uint160,uint128,int24)`**: `0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67`
* **`ClaimRewards(address,uint256)`**: `0x1f89f96333d3133000ee447473151fa9606543368f02271c9d95ae14f13bcc67`
* **`NotifyReward(address,uint256)`**: `0x095667752957714306e1a6ad83495404412df6fdb932fca6dc849a7ee910d4c1`

Configuration files specify the canonical signature string; topic0 is computed and validated at startup using `keccak256`.

### G. Reorg Handling & Revert Pathway (Fix for S7)
1. **Hash Ring**: Maintains a circular buffer of recent block numbers and hashes (512 blocks).
2. **Safe Staging**: Events emitted under the `safe` finality tag are tracked in `evml:{shard}:released` ZSet.
3. **Reconcile Diff**: When the `finalized` sweep scans past a range, any block whose hash diverges from the ring triggers a `type=revert` event with identical `(blockHash, txHash, logIndex)` to downstream consumers.

### H. Flashblocks Scope for v1 (Fix for S8)
Flashblocks preconfirmations are out of scope for v1. All stream data is derived from standard `newHeads`, `safe` finality tags (~26s latency), and `finalized` finality tags (~18m latency).