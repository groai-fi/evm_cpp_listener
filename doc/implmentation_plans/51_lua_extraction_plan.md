# Plan: move the Redis Lua scripts out of `lua_scripts.cpp`

**Goal:** the 4 scripts live as real `.lua` files in `evm_listener/src/sink/lua/`. CMake embeds them into the binary
at build time. The bytes Redis receives stay identical, so the `EVALSHA` hashes don't change.

**Not changing:** the `Script` enum, `script_name()` / `script_source()`, the call builders, the JSON codecs,
`ScriptRunner`, `RedisppConn`, the reply handling, and all tests apart from those listed in step 4.
There's no runtime file loading, so a script and the C++ that fills its `KEYS`/`ARGV` always ship together.

This follows the existing migration section in [lua/README.md](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/lua/README.md#L57-L80). The decisions below fill in what it leaves open.

## Current state (verified)

| Item | Fact |
|---|---|
| Scripts | 4 `R"lua(...)lua"` literals, [lua_scripts.cpp:14-127](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/lua_scripts.cpp#L14-L127) |
| Consumers | `script_source()` only, used by [script_runner.cpp:9](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/script_runner.cpp#L9) and [redispp_conn.cpp:108](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redispp_conn.cpp#L108) (`SCRIPT LOAD`) |
| Generated headers | Already done for `version.hpp` (`configure_file ... @ONLY` → `build/.../generated/`, which is on `evml_core`'s include path) |
| Lint | [tools/lint_lua.sh](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/tools/lint_lua.sh) already lints `src/sink/lua/*.lua` and the embedded literals |
| `.gitattributes` | `*.lua text eol=lf` is already set |
| Prod Docker | `COPY evm_listener/ ./evm_listener/` already picks up `src/sink/lua/*.lua` |
| Expected SHA1s | Pinned in the README table (emit `203d6a4f…`, ckpt `2cd0edcf…`, final_trim `e6207d83…`, lease `270a5c5f…`) |

## Steps

1. **Extract the files byte-for-byte.** Write `emit.lua`, `ckpt.lua`, `final_trim.lua` and `lease.lua` with a
   one-off script that prints `script_source(s)` from the current binary. Don't copy by hand, because the trailing
   newline is part of the SHA.
   → verify: `sha1sum src/sink/lua/*.lua` matches the README table.

2. **Generate the header in CMake** (`evm_listener/CMakeLists.txt`, next to the `version.hpp` block):
   - Add a new template, `src/sink/lua_sources.hpp.in`, with 4 constants
     (`inline constexpr const char* kEmitLua = R"lua(@EVML_LUA_EMIT@)lua";` and so on).
   - Use `file(READ ...)` for each `.lua` and `configure_file(... @ONLY)` to write `generated/sink/lua_sources.hpp`.
     With `@ONLY`, only `@VAR@` is replaced, the values aren't scanned again, and `${}` or `@` inside a script is safe.
   - Add a `FATAL_ERROR` at configure time if a file:
     - doesn't start with `#!lua\n`;
     - contains `)lua"`;
     - contains `\r`.
   - Add each `.lua` to `CMAKE_CONFIGURE_DEPENDS`, so editing one and running `make build` re-runs configure and
     rebuilds.

3. **Switch `lua_scripts.cpp` over.** Delete the 4 literals (lines 13–127) and add
   `#include "sink/lua_sources.hpp"`. `script_source()` returns the same names, so nothing else changes.

4. **Tests and docs:**
   - Add a SHA1 test (see decision A) in `redis_scripts_test.cpp`.
   - Update the README: change the status note and the "Where things live" row, delete the Migration section and
     keep the SHA table as a record.
   - Update the `lint_lua.sh` embedded branch (see decision B).

5. **Gate:** `make lint-lua` lists the 4 files as passing, `make cmake-configure && make build && make test` exit 0,
   and editing a comment in `ckpt.lua` and then running `make build` rebuilds `lua_scripts.cpp.o`.

## Decisions for you

| # | Question | Options | Recommendation |
|---|---|---|---|
| A | Pin SHA1s in a test? | (1) A one-time `sha1sum` check during the migration only; (2) a permanent unit test asserting the 4 SHA1s via OpenSSL `SHA1` | **(1).** A permanent pin fails on every intentional script edit, and the real-Redis tests already cover behaviour. Pick (2) if you want script edits to be a deliberate "update the hash" step. |
| B | `lint_lua.sh` embedded-literal branch | (1) Leave it; it becomes a no-op because 0 literals are found; (2) delete it | **(2).** Once migrated it's dead code, and it would hide a literal that someone added back by mistake. Change the "found N" check to require exactly the 4 files in `src/sink/lua/`. |
| C | One generated header or one per script | (1) One `lua_sources.hpp` holding all 4; (2) 4 headers, as the README suggests | **(1).** One template and one `configure_file`. It's simpler and works the same way. |

## Risks

- **A byte drift changes the SHA.** An editor adding a BOM, CRLF or no final newline would change the hash. That's
  harmless at runtime because `SCRIPT LOAD` runs on every connect, but it's caught anyway by step 1's check,
  `.gitattributes`, the lint and the CMake `\r` check.
- **The raw-string delimiter.** A script containing `)lua"` would break the generated header. This is caught at
  configure time with a clear message, not as a cryptic compile error.
- **Rollback** is trivial: `git revert`. Redis state isn't affected because the bytes are identical.
