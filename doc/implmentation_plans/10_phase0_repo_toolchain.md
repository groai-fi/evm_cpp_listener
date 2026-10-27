# Phase 0: Repo Hygiene & Toolchain (rev 3) — ✅ IMPLEMENTED & VERIFIED (see "Results" at the bottom)

**Goal:** inside the devcontainer (Debian bookworm), with Redis running as a **separate Docker container**, `conan install` + `cmake --preset` builds `evm_listener --version` from the **root** `CMakeLists.txt`.

## 0.1 Secrets: ✅ done by you, re-verified
None of the five `.env` secret values appear in any tracked or untracked repo file except `.env` itself. Keep the check as a pre-commit script: `tools/check_secrets.sh` greps each value from `.env*` against `git ls-files` + untracked files and must report 0 matches.

## 0.2 Env files
`evm_listener/.env.example` (tracked) holds placeholders only, for the variables this service reads:
```
# Base providers. Each one is ENABLED iff its variable is set (orders are in config/base.toml).
# WS chain: QuickNode → Infura → Alchemy   ·   getLogs chain: Infura → Alchemy → Ankr → public mainnet.base.org → QuickNode
BASE_RPC_URL_QUICKNODE=https://<name>.base-mainnet.quiknode.pro/<QUICKNODE_TOKEN>/
BASE_WSS_URL_QUICKNODE=wss://<name>.base-mainnet.quiknode.pro/<QUICKNODE_TOKEN>/
BASE_RPC_URL=https://base-mainnet.infura.io/v3/<INFURA_PROJECT_ID>
BASE_WSS_URL=wss://base-mainnet.infura.io/ws/v3/<INFURA_PROJECT_ID>
BASE_RPC_URL_ALCHEMY=https://base-mainnet.g.alchemy.com/v2/<ALCHEMY_KEY>     # quota-exhausted providers cool down and re-probe automatically
BASE_WSS_URL_ALCHEMY=wss://base-mainnet.g.alchemy.com/v2/<ALCHEMY_KEY>
BASE_RPC_URL_ANKR=https://rpc.ankr.com/base/<ANKR_KEY>
# Ethereum mainnet (future)
MAINNET_RPC_URL=  MAINNET_WSS_URL=  MAINNET_RPC_URL_ALCHEMY=  MAINNET_WSS_URL_ALCHEMY=
# Sink / ops
REDIS_URL=redis://redis:6379/0  # Railway: ${{Redis.REDIS_URL}}
EVML_HOST_ID=                   # a | b ; defaults to hostname
N8N_ALERT_WEBHOOK_URL=          # n8n "EVM Listener Alert Webhook" URL
N8N_ALERT_WEBHOOK_TOKEN=        # sent as X-API-Key; real value only in .env / Railway variables
DEADMAN_PING_URL=https://hc-ping.com/<CHECK_UUID>   # healthchecks.io; leader pings every 60 s; empty → disabled (WARN)
```
The real values go in `evm_listener/.env`, which is gitignored and already holds the provider URLs.
- **Add** `BASE_WSS_URL` (Infura `/ws/v3/<id>` form; verified working on 2026-10-05) and the two `N8N_*` values there. `DEADMAN_PING_URL` and `BASE_RPC_URL_ANKR` are already present.
- The `.env` you described at `evm_cpp_listener/.env` works too, because the loader walks up. Keep one copy to avoid drift.
- COINBASE_* and explorer keys are **not** read by this service.
- Add the new variable names (`BASE_WSS_URL*`, `N8N_ALERT_WEBHOOK_URL`, `N8N_ALERT_WEBHOOK_TOKEN`, `DEADMAN_PING_URL`) to the canonical list in the secrets rule.
- `tools/check_secrets.sh` also covers the n8n token.

## 0.3 Conan 2: root `conanfile.py`
Unchanged from rev 1: boost 1.90, openssl 3.5 LTS, simdjson 4.x, ethash 1.0.1, redis-plus-plus 1.3.15 (+hiredis, `with_tls=True` for `rediss://`), tomlplusplus 3.4, spdlog, prometheus-cpp 1.3 (`with_pull=False`), catch2 3.x. **No** evmone, secp256k1, intx or libsystemd. Commit `conan.lock`. Profile: `.devcontainer/conan/profiles/linux-gcc12` (bookworm gcc 12, `compiler.cppstd=20`).

## 0.4 CMake
- **Root `CMakeLists.txt`**, current state after your edit:
  - ✅ `//` comments are fixed.
  - ⚠️ `find_package(simdjson QUIET)` at the root **silently continues when simdjson is missing**. That violates fail-fast, and dependencies belong to the subproject. → Remove it from the root; use `find_package(simdjson REQUIRED)` in `evm_listener/CMakeLists.txt`.
  - ⚠️ `pkg_check_modules(UUID REQUIRED uuid)` is still at the root but the listener doesn't use it → remove it (`common/` has its own).
  - Add `option(EVML_BUILD_TESTS ON)` + `enable_testing()`. Keep the `SubProjects` loop.
- **`evm_listener/CMakeLists.txt`**: `evml_core` static library + `evm_listener` executable + `tests/`. Every dependency is `REQUIRED`. `-Wall -Wextra -Wpedantic -Werror` on our targets only. Presets: Debug (+ASan/UBSan) and RelWithDebInfo `-O2`, with no `-march=native`. Version from `git describe` → `version.hpp`. Lua scripts embedded into a generated header.

## 0.5 Devcontainer (keep bookworm; add Docker Redis)
Current: `mcr.microsoft.com/devcontainers/cpp:1-bookworm` + apt libs + Conan + in-container `redis-server`. Changes:
1. **Switch to compose** (`.devcontainer/docker-compose.yml`), and set `"dockerComposeFile"`, `"service": "dev"` and `"workspaceFolder"` in `devcontainer.json`:
   - `dev`: the existing Dockerfile.
   - `redis`: `redis:8.4` (pinned to Railway prod 8.4.0), `command: redis-server --appendonly yes --maxmemory-policy noeviction`, named volume.
   - `toxiproxy`: `ghcr.io/shopify/toxiproxy`, for chaos tests in Phase 5.
   - The listener reaches Redis at `redis://redis:6379` (or through toxiproxy).
2. Remove `postStartCommand: redis-server …` and the `redis-server` apt package (keep `redis-tools` for `redis-cli`).
3. ✅ **Approved:** remove the apt `libboost-all-dev libsimdjson-dev libsecp256k1-dev libsystemd-dev libhiredis-dev catch2 libgtest-dev libgmock-dev`. Conan 2 is then the only source of libraries (Boost 1.90, simdjson, OpenSSL, Catch2, …), so CMake can't pick bookworm's Boost 1.81 by accident.
4. Add Foundry (`anvil`, `cast`, pinned version) for e2e tests.
5. Forward 9464 (metrics/health). 6379 is published by the redis service.

## 0.6 .gitignore
Add `CMakeUserPresets.json`, `*.wal`, `**/wal/`.

## Acceptance
```bash
conan install . --build=missing -s build_type=Debug -pr:a=.devcontainer/conan/profiles/linux-gcc12
cmake --preset conan-debug && cmake --build --preset conan-debug
./build/Debug/evm_listener/evm_listener --version      # semver + git sha, exit 0
redis-cli -h redis PING                                 # PONG from the Docker Redis
ctest --preset conan-debug -L unit                      # placeholder test passes
tools/check_secrets.sh                                  # 0 matches
```

## Results (2026-10-06) — all acceptance commands exit 0 inside the devcontainer
- `conan install` (Debug, gcc 12, arm64) → `cmake --preset conan-debug` → build clean with `-Wall -Wextra -Wpedantic -Werror` + ASan/UBSan.
- `evm_listener --version` → `evm_listener 0.1.0 (<git describe>)`, exit 0.
- `redis-cli -h redis PING` → `PONG` (Docker Redis 8.4, not published to the host).
- `ctest -L unit` → 8/8 pass (keccak topic0 vector, simdjson, toml++, boost.json, spdlog, OpenSSL 3.5, redis++/prometheus link, version).
- `tools/check_secrets.sh` → 39 values, 0 matches.
- `conan.lock` generated; locked Debug install verified.

### Deliberate deviations from this plan
| Item | Decision |
|---|---|
| `common/` | Gated behind `EVML_BUILD_COMMON` (default OFF). It needs system hiredis/uuid/nlohmann_json that Conan no longer provides. Untouched otherwise. |
| toxiproxy | Deferred to Phase 5 (first user). |
| Foundry/anvil | Deferred to Phase 6 (first user). |
| `libssl-dev` | Also removed from apt (Conan owns OpenSSL). `uuid-dev` kept. |
| Boost | Compiled libs trimmed to `json container system charconv` (Asio/Beast header-only). Add to `_BOOST_KEEP` in `conanfile.py` if later code needs more. |
| hiredis | `with_ssl=True` is mandatory: redis-plus-plus `with_tls=True` is rejected otherwise (Conan error). |
| Link target | redis++ target is `redis++::redis++_static`. |
| git describe | Fails fast (FATAL_ERROR) if git is unusable; the Dockerfile marks the workspace `safe.directory` (bind-mount owner mismatch). Override with `-DEVML_GIT_DESCRIBE=` for builds without `.git`. |
| Compose | `dev` has no fixed `container_name` (it collided with the user's running legacy `evm_cpp_listener_dev`). Container is `devcontainer-dev-1`. |
| Docker CLI on macOS | Not on PATH; `docker-credential-desktop` also needs `/Applications/Docker.app/Contents/Resources/bin` on PATH. |
