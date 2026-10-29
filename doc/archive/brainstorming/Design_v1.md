# 整合提案:按合約分片的 24/7 C++ 事件監聽器(v1, Base 8453)

> [!NOTE]
> **Historical Pre-Implementation Brainstorming Draft (Archived):** This document reflects initial pre-Phase-0 brainstorming and contains superseded designs (see [`01_doc_review_findings.md`](../../implmentation_plans/01_doc_review_findings.md)). For the canonical v1 architecture and specification, see [`System_Modules_and_Architecture.md`](../../System_Modules_and_Architecture.md) and [`00_master_plan.md`](../../implmentation_plans/00_master_plan.md).

先修正一個前提:「按合約」分片不要做成**一個合約一個 process**。合約數一多就是幾十個 WS 連線、幾十倍 RPC 連線額度與記憶體,而 `eth_subscribe("logs")` 本來就支援 `address` 陣列。正確的單位是 **shard**:一個 process 負責一組合約(按事件量、業務重要性或下游消費者分組),一個 shard 一份 config、一個服務 instance。

## 1. 整體拓樸

```
WS Provider (QuickNode / Infura) ──┐
                                   ├─ evm-listener@base_eurc_usdc (config: base.toml, DEX pools & gauge)
                                   └─ evm-listener@registry       (config: registry.toml, Factory & Voter)
                                             │  每個 instance:Subscribing(buffer) → Backfill(gate) → Drain → Live
                                             │  /healthz HTTP & Docker HEALTHCHECK / sd_notify WATCHDOG / Prometheus
                                             ▼
                                      Redis 8.4 (Shared HA / Railway)
                                        ├─ Stream  evml:{shard}:events       ← XADD 事件 (MINID ~ 48h)
                                        ├─ Stream  evml:{shard}:registry     ← XADD 工廠/發現事件
                                        ├─ Hash    evml:{shard}:ckpt         ← {safe, finalized} last_processed_block
                                        ├─ String  evml:{shard}:dedup:...    ← SET NX EX 7d (含 :log / :revert)
                                        ├─ ZSet    evml:{shard}:released     ← safe 階段事件暫存 (用於 safe-reorg revert)
                                        └─ String  evml:{shard}:leader       ← Active-Passive 租約 (TTL 20s)
                                             ▼
                                      下游 consumer,用 consumer group 讀 (依 event_id 保持冪等)
```

多主機冗餘採用 **Active-Passive (Redis 租約 `evml:{shard}:leader`)** 模式:Leader 負責監聽與寫入,Standby 待命(~0 RPC),Leader 異常時 20s 內自動接管。

## 2. Config 設計(每個 shard 一份)

```toml
# config/base.toml
[shard]
name            = "base_eurc_usdc"
chain_id        = 8453        # Base Mainnet
start_block     = 20000000    # 首次啟動且 Redis 無 checkpoint 時使用

[rpc]
# WS 與 HTTP 分開配置; 支援多 provider 容錯
ws_endpoints = [
  { name = "quicknode", url = "wss://base-mainnet.quiknode.pro/${BASE_QUICKNODE_TOKEN}/" },
  { name = "infura",    url = "wss://base-mainnet.infura.io/ws/v3/${BASE_INFURA_KEY}" }
]
getlogs_endpoints = [
  { name = "infura",    url = "https://base-mainnet.infura.io/v3/${BASE_INFURA_KEY}", max_range = 500 },
  { name = "ankr",      url = "https://rpc.ankr.com/base/${BASE_ANKR_KEY}",            max_range = 10000 },
  { name = "public",    url = "https://mainnet.base.org",                              max_range = 500 },
  { name = "quicknode", url = "https://base-mainnet.quiknode.pro/${BASE_QUICKNODE_TOKEN}/", max_range = 5 }
]
stall_timeout_sec   = 15      # Base 出塊 2s; 無 newHeads 超過 15s (≈7 blocks) → 強制重連
max_getlogs_range   = 500     # 預設回補每批 block 數 (結合 AIMD 動態調整)

[filters]
# 支援按合約指定 finality 等級 (pools 走 safe ~26s, gauge/registry 走 finalized ~18m)
[[filters.contracts]]
name     = "eurc_usdc_pool_ts1"
address  = "0xc5E51044eB7318950B1aFb044FccFb25782C48c1"
finality = "safe"
events   = ["Swap", "Mint", "Burn", "Collect"]

[[filters.contracts]]
name     = "eurc_usdc_pool_ts50"
address  = "0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7"
finality = "safe"
events   = ["Swap", "Mint", "Burn", "Collect"]

[[filters.contracts]]
name     = "eurc_usdc_gauge_ts50"
address  = "0x1f6c9d116CE22b51b0BC666f86B038a6c19900B8"
finality = "finalized"
events   = ["Deposit", "Withdraw", "ClaimRewards", "NotifyReward", "ClaimFees"]

[sink]
redis_url      = "redis://default:${REDIS_PASSWORD}@${REDIS_HOST}:6379"
stream_key     = "evml:base_eurc_usdc:events"
retention_hrs  = 48           # XADD MINID ~ (now - 48h)
dedup_ttl_days = 7            # Dedup TTL ≥ 7 天 (容忍單機長期停機)
wal_path       = "/data/base_eurc_usdc.wal"
wal_max_mb     = 512

[metrics]
listen = "0.0.0.0:9101"
```

API key 與密碼一律透過 process 環境變數或 `.env` 注入,絕不硬編碼進 config 檔。

## 3. Binary 內部狀態機(生命週期與正確性核心)

```
INIT ──► CONNECTING ──► SUBSCRIBING (Buffer) ──► BACKFILL (to Gate) ──► DRAIN ──► LIVE
            ▲                   │                       │                 │         │
            └───────────────────┴───────────────────────┴─────────────────┴─────────┘
                       任何錯誤 / stall / ping 逾時 → 指數退避後回 CONNECTING
```

1. **CONNECTING**:
   - 建立 WS 連線,退避序列 250ms → 500ms → 1s … 上限 30s,加 Full-jitter。
   - 退避發生在 process 內,連上後發送 `eth_chainId` 驗證鏈 ID (8453)。
   - 若遭遇 HTTP 401/403 則標記 Fatal,429 QuotaExhausted 進入 1 小時冷卻並切換至下一 provider。

2. **SUBSCRIBING (Buffer)**:
   - **必須先訂閱,再記錄回補終點**: 同一條 WS 同時訂閱 `newHeads` 與 `logs`(帶過濾規則)。
   - 在回補完成前,即時收到的 live log 與 head 一律推入記憶體暫存佇列 (`pending_buffer`),確保「訂閱建立 → 回補開始」之間毫無時間縫隙。

3. **BACKFILL (to Gate)**:
   - 讀取 Redis checkpoint `evml:{shard}:ckpt` 中的 `{safe, finalized}` 高度。
   - **向 getLogs provider 查詢其自身的 safe / finalized finality tag (記為 $G_s, G_f$)**。
   - 回補**絕不直接打到 latest**,而是精確補到對應的 finality gate:
     - Safe 類事件從 $C_s$ 補到 $G_s$。
     - Finalized 類事件從 $C_f$ 補到 $G_f$。
   - 使用 AIMD 動態分頁請求 `eth_getLogs`。回補期間暴露 `backfill_in_progress=1` 指標。

4. **DRAIN**:
   - 依 `(blockNumber, logIndex)` 排序並消化 `pending_buffer` 中的即時事件,推進至即時流。

5. **LIVE (Steady State)**:
   - 每個 `newHeads` → 更新本地 head、重置 stall timer (15s 看門狗)。
   - **Finality 門檻推進 (Gate Sweep & Checkpoint)**:
     - 獨立於事件發生頻率,定時 (例如每 4s 輪詢 safe tag、每 12s 輪詢 finalized tag) 推進 finality gate。
     - **Checkpoint 隨 finality gate 的成功 sweep 推進,而非僅依賴是否有 log 事件** (即使數天無事件,checkpoint 依然緊跟鏈上 finalized 高度,重啟時絕不重複回補數萬區塊)。
   - **即時日誌處理與去重**:
     - 透過 Redis Lua Script 進行原子 `SET NX` 去重 + `XADD` 寫入。
   - **Reorg 偵測與 Revert 路徑**:
     - 監聽 `newHeads` 維護最近 512 塊的 canonical hash ring。
     - Safe 類事件在未達 finalized 前記錄於 `evml:{shard}:released` ZSet; 若 finalized sweep 比對發現分叉(hash changed 或 log 被撤銷),自動派發 `type=revert` 事件。
   - **防假死交叉校驗**:
     - 定期(每 60s)比對 WS head 與 HTTP getLogs provider 的 `eth_blockNumber`,若 WS 靜默未推 log 則由 sweep 機制主動對帳補齊。

6. **SIGTERM / 優雅關閉**:
   - 停止接收新事件 → flush WAL/queue 到 Redis → 寫入最新 checkpoint → 釋放 Leader 租約 → exit 0。

## 4. Redis 資料模型與原子去重

```
evml:{shard}:events       Stream, 欄位: type(log|revert) finality(safe|finalized) block blockHash tx logIndex address topic0 data raw_json
evml:{shard}:registry     Stream, 工廠/治理事件 (PoolCreated, GaugeCreated, GaugeKilled, GaugeRevived)
evml:{shard}:ckpt         Hash,   欄位: safe="52210305", finalized="52209779" (HSET-if-greater)
evml:{shard}:dedup:{blockHash}:{txHash}:{logIndex}:{type}   String, SET NX EX 604800 (TTL 7天)
evml:{shard}:released     ZSet,   score=blockNumber, member="blockHash:txHash:logIndex" (safe 暫存)
evml:{shard}:leader       String, SET NX EX 20 (Active-Passive 租約)
```

### 原子去重原則:
- **必須使用 Redis Lua Script / Redis Function**,不能使用 `MULTI/EXEC` (因為 `MULTI` 無法根據 `SET NX` 的執行結果決定是否跳過 `XADD`)。
- 去重 Key 的後綴**必須包含 `:type`** (例如 `:log` 與 `:revert`),確保同一個 logIndex 被 revert 時,`revert` 事件不會被 `SET NX` 誤當成重複資料而靜默丟棄。
- Dedup TTL 設為 7~14 天,遠大於單機可能停機維護的時間。

## 5. 下游故障 Fallback 與本地 WAL

- **佇列緩衝**: Redis 暫時斷線時,事件寫入記憶體 bounded queue (容量 10k 筆)。
- **二進位/CRC WAL**: 佇列飽和時無縫溢流至磁碟 WAL (`/data/{shard}.wal`),WAL 包含 CRC32 校驗、序列號、自動滾動與最大容量限制 (如 512 MB)。
- **背壓 (Backpressure)**: 當 WAL 達到容量上限時,暫停從 WS 取事件。寧可觸發 stall 重啟並靠 checkpoint 回補,也絕不靜默丟失任何事件。
- **恢復重放**: Redis 連線恢復後,先按序重放 WAL,再清空記憶體佇列。

## 6. 可觀測性與告警體系

### Prometheus `/metrics` 核心指標:
| 指標 | 告警條件 | 說明 |
|---|---|---|
| `head_lag_seconds` (now − 最後 newHeads 時間) | > 15s (Base) | WS 連線可能已中斷或 stall |
| `gate_lag_blocks` (WS head − finalized gate) | > 800 blocks | Finality 推進停滯或 provider 落後 |
| `backfill_in_progress` | 持續 > 10 分鐘 | 回補異常或 range 過大 |
| `ws_reconnects_total` | 15 分鐘內 > 5 | 網路抖動或 provider 異常 |
| `reverts_total` | 短時間突增 | 鏈上發生深層 reorg (需人工介入) |
| `wal_bytes` / `queue_depth` | > 0 持續 2 分鐘 | Redis 寫入異常或磁碟累積 |
| `rpc_errors_total{code}` | 429 出現即告警 | RPC 額度耗盡 (Quota Exhausted) |

> [!WARNING]
> **絕對不要以 `seconds_since_last_event` 作為健康度告警!**
> 低頻合約(如每日僅數筆事件)會造成常態誤告警。健康度與 Liveness 一律依據 **`newHeads` 時間戳、Finality Gate 推進狀態、Heartbeat ping** 判定。

### 外部告警與 Dead-Man's Switch:
- 整合 **n8n Webhook** 派發結構化告警至 Slack / Telegram。
- 整合 **healthchecks.io** (Dead-man's switch, 60s 週期): 僅由健康的 Leader 發送 ping; 當進程死鎖或雙節點皆崩潰時,3 分鐘內由第三方服務主動觸發 Pager 告警。

## 7. 技術選型 (Production Stack)

- **WebSocket / HTTP**: **Boost.Beast + Boost.Asio** (C++20, 單執行緒 event loop 或 strand 隔離, 足以承載數萬 ops/s)。
- **JSON 解析**: **simdjson** (高效能唯讀解析, 預留 `SIMDJSON_PADDING` 記憶體對齊緩衝區)。
- **JSON 生成**: **Boost.JSON** 或格式化字串建構器 (simdjson 為純解析器, 不支援序列化)。
- **Redis 客戶端**: **redis-plus-plus** (基於 hiredis, 支援連線池、Lua 腳本與 Pipeline)。
- **編譯與依賴管理**: **CMake + Conan 2 (Conan 2.x)**。
- **連結方式**: 靜態連結第三方依賴庫 (Boost, simdjson, hiredis 等), **動態連結系統 glibc / libstdc++** (避免完全靜態連結導致 NSS / `getaddrinfo` DNS 解析損壞)。
- **測試框架**:
  - 單元/整合測試: 內建 mock HTTP/WS server 進行故障注入。
  - 端到端測試: **Foundry Anvil** (`anvil_reorg`, `evm_mine`) 模擬鏈重組與斷線。

## 8. 部署架構 (Railway 容器化與 Systemd)

### A. Railway 雲端容器部署 (推薦)
- 使用 Docker 容器化, 暴露 `/healthz` (HTTP 200) 與 `/metrics` (Prometheus)。
- 配置 Docker `HEALTHCHECK` 指令定時探測 `/healthz`。
- 多實例部署 (Leader-Standby 雙節點), 透過 Redis Lease 自動選主。

### B. Linux Bare-Metal / VM (Systemd 部署)
```ini
# /etc/systemd/system/evm-listener@.service
[Unit]
Description=EVM Event Listener Shard %I
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=notify
ExecStart=/usr/local/bin/evm-listener --config /etc/evm-listener/%I.toml
Restart=always
RestartSec=2
WatchdogSec=30
NotifyAccess=main
User=evmlistener
Group=evmlistener
EnvironmentFile=/etc/evm-listener/%I.env
LimitNOFILE=65536
MemoryMax=1G
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/data /var/lib/evm-listener
KillSignal=SIGTERM
TimeoutStopSec=20
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

## 9. 多 Provider 容錯矩陣

1. **WS 訂閱鏈**: `QuickNode (Active) ──failover──► Infura (Standby)`。
2. **getLogs 回補鏈**: `Infura (max 500) ──► Ankr (max 10,000) ──► Public Base (max 500) ──► QuickNode (max 5)`。
3. **動態啟用**: 只要對應環境變數未配置或已耗盡, 自動降級並冷卻, 額度重置後自動重新加入。