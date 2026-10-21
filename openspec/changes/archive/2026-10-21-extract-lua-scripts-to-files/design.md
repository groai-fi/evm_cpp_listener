# Design

## Context

`lua_scripts.cpp` lines 13–127 hold the four literals. `script_source()` is the only consumer (`script_runner.cpp`, `redispp_conn.cpp` call `SCRIPT LOAD` with it). `version.hpp.in` is already generated into `${CMAKE_CURRENT_BINARY_DIR}/generated`, which is on `evml_core`'s include path. `tools/lint_lua.sh` already lints standalone files in `src/sink/lua/`, `.gitattributes` pins `*.lua` to LF, and the prod Dockerfile copies `evm_listener/`.

The EVALSHA hash is the SHA1 of the exact bytes, including the trailing newline. Pinned values (README): emit `203d6a4f…`, ckpt `2cd0edcf…`, final_trim `e6207d83…`, lease `270a5c5f…`.

## Goals / Non-Goals

**Goals:**
- Scripts are real `.lua` files; the embedded bytes are identical to today's.
- A bad script fails at configure time with a clear message.

**Non-Goals:**
- No change to script logic, `KEYS`/`ARGV` layout, reply values, or call builders.
- No runtime file loading.
- No permanent SHA1 unit test (see decisions).

## Decisions

1. **One generated header, `sink/lua_sources.hpp`**, from `src/sink/lua_sources.hpp.in` with four `@EVML_LUA_*@` placeholders. Alternative: one header per script (as the README suggested); rejected as more CMake for no benefit.
2. **`configure_file(... @ONLY)`** with `file(READ ...)`. `@ONLY` does not rescan substituted values, so `@` or `${}` inside a script is safe. The same pattern as `version.hpp`.
3. **Validation in CMake** (`FATAL_ERROR`) for the `#!lua\n` prefix, `)lua"` and `\r`. Alternative: leave it to `make lint-lua`; rejected because a `)lua"` would surface as a cryptic compiler error.
4. **`CMAKE_CONFIGURE_DEPENDS`** lists each `.lua`, so edits trigger configure and rebuild.
5. **Extraction is done by printing `script_source()` from the current build**, not by hand copying, so the trailing newline is preserved. Verified once with `sha1sum` against the README table. No permanent SHA test: it would fail on every intentional script edit; behaviour is covered by the real-Redis tests.
6. **`lint_lua.sh`**: delete the embedded-literal branch; require exactly the four files in `src/sink/lua/`. A literal re-added to C++ would otherwise go unlinted.

## Risks / Trade-offs

- A BOM, CRLF or missing final newline changes the SHA. Harmless at runtime (`SCRIPT LOAD` runs per connect) and caught by `.gitattributes`, the CMake `\r` check and the lint.
- `)lua"` in a script breaks the generated header: caught at configure time.
- Rollback is a plain `git revert`; Redis state is unaffected because the bytes are identical.
