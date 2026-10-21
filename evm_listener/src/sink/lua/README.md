# Redis Lua scripts

The 4 scripts the listener runs in Redis with `EVALSHA`: `emit`, `ckpt`, `final_trim`, `lease`.

> **Embedding:** `evm_listener/CMakeLists.txt` reads the four `.lua` files at configure time, validates them, and generates
> `generated/sink/lua_sources.hpp` from [`../lua_sources.hpp.in`](../lua_sources.hpp.in). `lua_scripts.cpp` includes it.
> Editing a `.lua` and running `make build` re-runs configure and rebuilds the binary.

## Where things live

| What | Where |
|---|---|
| Script body + its `KEYS`/`ARGV` layout (header comment) | `<name>.lua` here |
| C++ that fills `KEYS`/`ARGV` (`emit_call`, `ckpt_*_call`, `final_trim_call`, `lease_*_call`) | [`../lua_scripts.cpp`](../lua_scripts.cpp) |
| Allowed reply values per script | [`../lua_scripts.hpp`](../lua_scripts.hpp) |
| Real-Redis tests for every script | [`../../../tests/redis_scripts_test.cpp`](../../../tests/redis_scripts_test.cpp) |
| Lint rules (Redis sandbox globals) | [`/.luacheckrc`](../../../../.luacheckrc), [`/tools/lint_lua.sh`](../../../../tools/lint_lua.sh) |

**Changing the `KEYS`/`ARGV` layout or a reply value is a contract change**: update the C++ call builder, the
reply handling, and the tests in the same change. The scripts are compiled into the binary, so script and C++ always
ship together.

## Rules (enforced by `make lint-lua`)

1. **First line is exactly `#!lua`** (flags allowed, `allow-oom` forbidden). Without it Redis runs the script in
   legacy mode: on OOM it can fail after the first write, and Redis never rolls back, leaving partial writes.
   No license header, BOM or blank line before it.
2. **LF line endings, UTF-8 without BOM.** `.gitattributes` pins `*.lua` to LF.
3. **Lua 5.1 only.** No `//`, `goto`, `<const>`, bit operators; use the `bit` library.
4. **Only Redis sandbox globals.** No `print`, `require`, `io`, `os.time` (only `os.clock`). Use
   `redis.log(redis.LOG_NOTICE, ...)` for debugging. The exact list is in `.luacheckrc`.
5. **No global variables.** Use `local`; Redis rejects global writes at runtime.
6. **Never contain `)lua"`** — it ends the C++ raw string the script is embedded in.

## Conventions (not linted; keep them)

- **Fence check first.** `emit`, `ckpt` and `final_trim` start with the leader-token check
  (`token ~= '0' and GET leader ~= token` → `FENCED`) before any write. `lease` is the script that manages that key.
- **Write the idempotency marker last.** Redis does not roll back a failed script. `emit` writes its dedup key after
  everything else, so a failure causes a retry (allowed duplicate), never a silent loss.
- **Replies are strings or nil.** Convert numbers with `tostring`; the C++ side parses strings only.
- **Validate before writing** (e.g. `ckpt` checks finalized ≤ safe before `HSET`), and return
  `redis.error_reply('ERR ...')` for contract violations.

## Workflow (inside the dev container: `make shell`)

```bash
make lint-lua                 # luacheck + Lua 5.1 parse + invariants, seconds
make test                     # includes redis_scripts_test: every script against real Redis 8.4
# Ad-hoc run against the compose Redis (DB 15 to stay clear of app data). Keys before ',', args after:
redis-cli -u "$REDIS_URL" -n 15 --eval emit.lua key1 key2 , arg1 arg2
```

> [!NOTE]
> The EVALSHA hash is the SHA1 of the exact bytes, including the trailing newline. `SCRIPT LOAD "$(cat f.lua)"`
> strips it and yields a different SHA than the binary uses.

## Script hashes (EVALSHA)

Unchanged by the move from C++ literals to files (measured before and after):

| Script | SHA1 | Bytes |
|---|---|---|
| emit | `203d6a4f30d6df453a692ffd2603d0d982744303` | 1242 |
| ckpt | `2cd0edcf7a15401da3a9e760b323ac4fad3ec7b0` | 1422 |
| final_trim | `e6207d83d31f58ebceeb73f03c335b3b2d70c450` | 436 |
| lease | `270a5c5fec78258cc6b96b09902bf1941533f1e4` | 1166 |

These change whenever a script is edited on purpose; update the table then.
