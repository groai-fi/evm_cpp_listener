# Proposal

## Why

The four Redis Lua scripts (`emit`, `ckpt`, `final_trim`, `lease`) are embedded as `R"lua(...)lua"` string literals in `evm_listener/src/sink/lua_scripts.cpp`. Editors can't syntax-highlight or lint them as Lua, and they are mixed with the C++ call builders. `src/sink/lua/README.md` already documents the target layout (real `.lua` files embedded at build time) as a pending migration.

## What Changes

- Move each script, byte-for-byte, into `evm_listener/src/sink/lua/{emit,ckpt,final_trim,lease}.lua`.
- CMake reads the four files and generates one header, `generated/sink/lua_sources.hpp`, with `configure_file(... @ONLY)`.
- CMake fails at configure time if a script does not start with `#!lua\n`, contains `)lua"`, or contains `\r`.
- Editing a `.lua` file re-runs configure on the next `make build` and rebuilds the binary.
- `lua_scripts.cpp` includes the generated header instead of holding the literals. `script_name()` / `script_source()` and all callers are unchanged.
- `tools/lint_lua.sh`: remove the branch that scans embedded literals. Require exactly the four files in `src/sink/lua/`.
- `src/sink/lua/README.md`: update the status note and remove the Migration section. Keep the SHA1 table.
- No runtime file loading. The `EVALSHA` hashes stay the same, so Redis sees identical script bytes.

## Capabilities

### New Capabilities

### Modified Capabilities
- `build-and-packaging`: adds a requirement that the Redis Lua scripts live as standalone files and are embedded into the binary at build time, with configure-time validation.

## Impact

- Code: `evm_listener/CMakeLists.txt`, `evm_listener/src/sink/lua_scripts.cpp`, new `evm_listener/src/sink/lua_sources.hpp.in`, new `evm_listener/src/sink/lua/*.lua`.
- Tooling and docs: `tools/lint_lua.sh`, `evm_listener/src/sink/lua/README.md`.
- No change to Redis state, wire format, public headers (`lua_scripts.hpp`), tests' expectations, or the prod Dockerfile (it already copies `evm_listener/`).
