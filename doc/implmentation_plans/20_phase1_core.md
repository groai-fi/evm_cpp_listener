# Phase 1: Core (`evml::core`)

**Goal:** pure, deterministic building blocks with no network. 100% unit-testable.

## 1.1 Env loader: `core/env.hpp`
- `env::load_dotenv()` walks up from the cwd over `.env`, `.env.local.dev`, `.env.local`. It never overwrites existing env vars (per the project rule). It strips trailing `\r`, because the existing `evm_listener/.env` has **CRLF** line endings (verified); a URL ending in `\r` would fail TLS/host parsing.
- `env::require(name)` throws `ConfigError("missing env: NAME")`.
- `redact(url)` masks the path/query after the host (`wss://base-mainnet.g.alchemy.com/v2/****`). It is used by **every** log line that mentions an endpoint.

## 1.2 Config: `core/config.hpp` (toml++)
The config references **env var names**, never secret values.

```toml
# evm_listener/config/base.toml   (v1, values verified on-chain 2026-10-05, see 02)
[shard]
id       = "base_eurc_usdc"         # Aerodrome Slipstream EURC/USDC. Redis namespace {base_eurc_usdc}, alert `shard`
host_id  = ""                       # empty → $EVML_HOST_ID → hostname. Alert `host`

[chain]
name = "base"   chain_id = 8453   block_time_ms = 2000
default_finality = "finalized"      # finalized | safe | depth; per-contract override below (W3 = c)
depth          = 0                  # only for finality = "depth" (Anvil tests)
hash_ring_size = 1024
start_policy   = "contract_start"   # no ckpt → min(contract.start_block); or "gate" (= now)
safe_poll_ms   = 4000               # safe tag poll (drives safe-class latency)
finalized_poll_ms = 12000

# A provider is ENABLED iff its env var(s) are non-empty (missing → INFO "provider skipped: env X unset").
# ws_env set → WS candidate; http_env/http_url set → getLogs + tags candidate.
[[providers]]
name = "quicknode"   ws_env = "BASE_WSS_URL_QUICKNODE" http_env = "BASE_RPC_URL_QUICKNODE" max_getlogs_range = 5      # measured: Discover plan, -32615
[[providers]]
name = "infura"      ws_env = "BASE_WSS_URL"           http_env = "BASE_RPC_URL"           max_getlogs_range = 500    # measured: 2000 → HTTP 429
[[providers]]
name = "alchemy"     ws_env = "BASE_WSS_URL_ALCHEMY"   http_env = "BASE_RPC_URL_ALCHEMY"   max_getlogs_range = 2000   # quota exhausted today → auto cooldown/re-probe
[[providers]]
name = "ankr"                                          http_env = "BASE_RPC_URL_ANKR"      max_getlogs_range = 10000  # measured: 10k OK
[[providers]]
name = "base_public"                                   http_url = "https://mainnet.base.org" max_getlogs_range = 500  # no secret; measured: -32614 above 500; best effort

[redundancy]
mode                 = "failover"   # failover (v1) | hedged (supported, off)
ws_order             = ["quicknode", "infura", "alchemy"]
getlogs_order        = ["infura", "alchemy", "ankr", "base_public", "quicknode"]   # first healthy one ≠ active WS; else any healthy
failover_after       = 3            # consecutive session failures
stale_after_blocks   = 5            # WS head behind the getLogs provider's head → WS failover
prefer_primary_after_s = 600        # return to the first entry after 10 min healthy (anti-flap)
quota_cooldown_s     = 3600         # QuotaExhausted → skip provider, re-probe hourly

[lease]                             # cross-host active-passive
enabled = true   ttl_ms = 15000   renew_ms = 5000

[[contracts]]                       # ts=1: high-volume swap pool (gauge killed → gauge NOT tracked)
name = "eurc_usdc_cl1_pool"   address = "0xc5E51044eB7318950B1aFb044FccFb25782C48c1"  start_block = 0  # fill: deploy block
finality = "safe"                   # all pool events at safe (decided)
events = [ "Swap(address,address,int256,int256,uint160,uint128,int24)",
           "Mint(address,address,int24,int24,uint128,uint256,uint256)",
           "Burn(address,int24,int24,uint128,uint256,uint256)",
           "Collect(address,address,int24,int24,uint128,uint128)" ]
[[contracts]]                       # ts=50: incentivised LP pool
name = "eurc_usdc_cl50_pool"  address = "0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7"  start_block = 0
finality = "safe"
events = [ "Swap(address,address,int256,int256,uint160,uint128,int24)",
           "Mint(address,address,int24,int24,uint128,uint256,uint256)",
           "Burn(address,int24,int24,uint128,uint256,uint256)",
           "Collect(address,address,int24,int24,uint128,uint128)" ]
[[contracts]]                       # ts=50 live gauge; finality = default (finalized)
name = "eurc_usdc_cl50_gauge" address = "0x1f6c9d116CE22b51b0BC666f86B038a6c19900B8"  start_block = 0
events = [ "Deposit(address,uint256,uint128)", "Withdraw(address,uint256,uint128)",
           "ClaimRewards(address,uint256)", "NotifyReward(address,uint256)",
           "ClaimFees(address,uint256,uint256)" ]
[[contracts]]
name = "aero_cl_factory"  address = "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A"  stream = "registry"
events = [ "PoolCreated(address,address,int24,address)" ]           # topic0 to confirm from a live log
[[contracts]]
name = "aero_voter"       address = "0x16613524e02ad97eDfeF371bC883F2F5d6C480A5"  stream = "registry"
events = [ "GaugeCreated(address,address,address,address,address,address,address,address)",
           "GaugeKilled(address)", "GaugeRevived(address)" ]       # topic0s to confirm from live logs
# registry contracts default start_block = "gate" (no deep history needed)

[backfill]
min_range = 10   request_timeout_ms = 15000

[watchdog]
head_stall_ms = 15000   subscribe_timeout_ms = 10000   progress_stall_ms = 120000
safe_gate_stuck_s = 600   gate_lag_alert_s = 3600   process_stall_ms = 30000   # last = self-abort threshold

[sink]
redis_url_env   = "REDIS_URL"
retention_ms    = 172800000         # XADD MINID ~ now−48h ("downstream down a few hours" × ~8)
dedup_ttl_s     = 604800            # 7 d
queue_capacity  = 100000
wal_dir         = "/data/wal"       # Railway volume; local: compose volume
wal_max_bytes   = 536870912
require_noeviction = true           # fail fast if Redis may evict keys

[ops]
http_listen  = "[::]:9464"          # $PORT honoured; IPv6 for Railway private network
heartbeat_s  = 10
alert_webhook_env       = "N8N_ALERT_WEBHOOK_URL"     # n8n → Slack #strategy-production
alert_webhook_token_env = "N8N_ALERT_WEBHOOK_TOKEN"   # sent as header X-API-Key; never logged
alert_service           = "evm-listener"
deadman_ping_env        = "DEADMAN_PING_URL"          # healthchecks.io; empty → skipped with a startup WARN (never silent)
deadman_ping_s          = 60                          # leader ∧ healthy only (Phase 5 §5.3.2)
http_user_agent         = "evm-listener/{version}"    # explicit UA: Cloudflare (mainnet.base.org) 403/1010 on some default UAs
```

Events are written as **signatures** in v1, because `expect_topic0` is redundant once the topic0 values are computed. The verified topic0 table goes in `config/README` and into unit tests, so a wrong signature fails CI.

**Validation (fail fast, exit code 2, precise message):**
- every address is 20-byte hex **and** (online `--check-config`) has code (`eth_getCode` ≠ `0x`). This would have caught 02-E1.
- at least one enabled WS provider (ws env set) and at least one enabled getLogs provider. Otherwise exit 2 with the list of **env var names** that are unset (values are never logged).
- every name in `ws_order` / `getlogs_order` refers to a declared provider. A `ws_order` entry with no `ws_env`, or a provider whose env is unset, is skipped with INFO, not an error. That is what makes "set the env var to enable" work.
- `http_url` literals are only for credential-free public endpoints (here, `mainnet.base.org`). Anything keyed goes through `*_env`.
- `finality=depth` requires depth > 0. A per-contract `finality` must be `safe|finalized` (or `depth` with chain depth > 0).
- If a webhook URL env var is set, its token env var must also be set, otherwise exit 2 (fail fast; no unauthenticated POSTs).
- no duplicate contract/event entries.
- an event signature is canonical: no spaces, no param names.
- the `stream` override must be one of `events|registry`.
- `chain_id` is checked against `eth_chainId` on every connection.

Normalise addresses to lowercase and dedupe topic0. The result is **one** `logs` filter `{address:[…], topics:[[t0…]]}` plus a reverse map `(address, topic0) → (contract, event, stream, finality)`. The safe sweep uses the subset filter of the safe-class addresses (Phase 3 §3.5). The topic0 OR-list is shared across addresses. That is safe here because no tracked contract emits another's events, and the reverse map drops any unexpected `(address, topic0)` pair (counted in a metric).

**`start_block` helper:** `evm_listener --find-deploy-block <addr>` binary-searches `eth_getCode` to find each contract's deploy block, so the config stays exact.


## 1.3 Hex / hashing: `core/hex.hpp`, `core/keccak.hpp`
- `parse_u64_hex`, `to_hex_u64`, `Hash32` / `Address20` value types (fixed arrays, hashable, `==`), strict parsing that rejects missing `0x`, odd length and overflow.
- `topic0(sig)` = `ethash::keccak256(sig)`. Signature canonicalisation strips spaces and rejects named params.

## 1.4 JSON codec: `rpc/codec.hpp`
- **Inbound** (simdjson On-Demand): one `simdjson::ondemand::parser` plus a reusable padded buffer per connection. The parser classifies messages as
  - `Response{id, result_raw | error{code,message,data}}`
  - `Notification{subscription_id, kind: Head|Log, payload}`
- Typed extractors: `Head{number, hash, parent_hash, timestamp}` and `Log{address, topics[≤4], data, block_number, block_hash, tx_hash, tx_index, log_index, removed}`. `raw_json` is kept for diagnostics only.
- Strict: a missing field becomes `DecodeError` with context, counted in a metric. **Never** silently defaulted to `0x0`, which the sample did.
- **Outbound**: Boost.JSON builders for `eth_subscribe`, `eth_getLogs`, `eth_getBlockByNumber`, `eth_blockNumber`, `eth_chainId`, `eth_unsubscribe`. Ids are a monotonic `uint64`.

## 1.5 Logging: `core/log.hpp`
- spdlog writes one JSON object per line to **stdout**, with the keys `level` and `message` (the shape Railway parses as structured logs) plus `ts, component, provider, state, …kv`.
- Lifecycle events go to INFO (state changes, reconnects, gate moves every N). Each emitted event goes to DEBUG. Rate-limited WARN for repeated errors.

## Tests (Catch2, label `unit`)
- topic0 vectors: `Transfer(address,address,uint256)` = `0xddf252ad…b3ef`, plus **every v1 event, using the values verified on-chain in 02** (Swap `0xc42079f9…5fbcca67`, Deposit `0x1c8ab8c7…`, Withdraw `0x8903a5b5…`, ClaimRewards `0x1f89f963…`, NotifyReward `0x09566775…`, ClaimFees `0xbc567d6c…`, Mint, Burn, Collect).
- Config: `base.toml` parses into 5 contracts (2 safe, 3 finalized); each invalid case gives an exact error message.
- Env loader: CRLF `.env` → values have no trailing `\r`; an existing env var is not overwritten.
- Codec: fixtures captured from real provider messages (`tests/fixtures/*.json`): heads, logs, `removed:true`, plus the **error shapes observed on 2026-10-05**: Alchemy `{"code":429,"message":"Monthly capacity limit exceeded…"}`, Infura HTTP 429 on a 2000-block getLogs, Infura `{"code":-32603,"message":"service temporarily unavailable"}`, QuickNode HTTP 413 `{"code":-32615,"message":"eth_getLogs is limited to a 5 range…"}`, and public Base HTTP 413 `{"code":-32614,"message":"eth_getLogs is limited to a 500 range"}`.
- Config: providers with unset env are skipped and the remaining order is correct; `base.toml` with only `BASE_RPC_URL_ANKR` set exits 2 ("no WS provider").
- Fixture capture task (Phase 1, online, one-off): pull one real log each for PoolCreated / GaugeCreated / GaugeKilled to confirm their topic0 values (02 §D).
- Redaction: no fixture URL survives `redact()` with its key intact.

## Acceptance
`ctest -L unit` is green under ASan/UBSan, and `evm_listener --config config/base.toml --check-config` (online) prints the filter and topic0 table, verifies that every address has code, and exits 0.

## Results and deviations (2026-10-06)
- Verified: 50/50 `ctest -L unit` green under ASan/UBSan; `tools/check_secrets.sh` OK (39 values, 0 matches).
- `--check-config` online (Docker, live Base): chainId 8453 on infura/ankr/base_public/quicknode; all 5 contracts have code; exit 0. Alchemy returned HTTP 429 (monthly quota exhausted) and is reported as WARN, as designed.
- `--find-deploy-block <pool1>` returned 21861234 (matches the `eth_getCode` search done by hand).
- `--bogus` gives usage, exit 2. `--version` unchanged.
- Deviations: CLI lives in `src/app/tools.{hpp,cpp}` (in `evml_core`, so it is testable). `blocking_http` uses a coroutine because Beast `tcp_stream` timeouts only cover async ops.
- `log.cpp` timestamp buffer enlarged to 96 bytes (GCC `-Wformat-truncation` worst case under `-Werror`).
- Not done: live confirmation of the `PoolCreated`/`GaugeCreated`/`GaugeKilled`/`GaugeRevived` topic0s (computed with `cast`, matching recalled prefixes).
- Open items for the user: `BASE_WSS_URL` and `N8N_*` are unset in `.env`; the QuickNode subdomain is visible in `redact()` output (only the path is masked).
