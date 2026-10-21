# Redis Lua scripts

The 4 scripts the listener runs in Redis with `EVALSHA`: `emit`, `ckpt`, `final_trim`, `lease`.

> **Status:** the scripts are still embedded as `R"lua(...)lua"` literals in [`../lua_scripts.cpp`](../lua_scripts.cpp).
> This directory is their target home (`emit.lua`, `ckpt.lua`, `final_trim.lua`, `lease.lua`). See [Migration](#migration-pending).

## Where things live

| What | Where |
|---|---|
| Script body + its `KEYS`/`ARGV` layout (header comment) | `<name>.lua` here (today: `lua_scripts.cpp`) |
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

## Migration (pending)

Delete this section once done. Target: scripts live here as files, the binary still embeds them at build time
(no runtime file loading, so script and C++ builders can't drift apart).

1. Move each literal body **byte-for-byte** into `emit.lua`, `ckpt.lua`, `final_trim.lua`, `lease.lua`.
2. CMake (`evm_listener/CMakeLists.txt`) generates one header per script with `configure_file` (same pattern as
   `version.hpp`), adds each `.lua` to `CMAKE_CONFIGURE_DEPENDS` so editing it re-runs configure on `make`, and
   `FATAL_ERROR`s if a file doesn't start with `#!lua\n` or contains `)lua"`.
3. `lua_scripts.cpp` includes the generated headers instead of the literals. `script_name()`, `script_source()`
   and all callers stay unchanged.
4. Acceptance:
   - `make lint-lua` passes and lists the 4 files (it already lints this directory; the embedded part becomes a no-op).
   - `make test` passes.
   - `script_source()` SHA1s are unchanged (EVALSHA hashes), measured from the literals before the move:

     | Script | SHA1 | Bytes |
     |---|---|---|
     | emit | `203d6a4f30d6df453a692ffd2603d0d982744303` | 1242 |
     | ckpt | `2cd0edcf7a15401da3a9e760b323ac4fad3ec7b0` | 1422 |
     | final_trim | `e6207d83d31f58ebceeb73f03c335b3b2d70c450` | 436 |
     | lease | `270a5c5fec78258cc6b96b09902bf1941533f1e4` | 1166 |

   - Editing a `.lua` file and running `make build` rebuilds the binary with the new script.
