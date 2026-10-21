# Tasks

## 1. Extract the scripts

- [x] 1.1 Dump `script_source()` for the 4 scripts from the current build into `evm_listener/src/sink/lua/{emit,ckpt,final_trim,lease}.lua`
- [x] 1.2 Verify `sha1sum` and byte counts match the README table (emit 1242, ckpt 1422, final_trim 436, lease 1166)

## 2. CMake embedding

- [x] 2.1 Add `src/sink/lua_sources.hpp.in` with four raw-string constants
- [x] 2.2 In `evm_listener/CMakeLists.txt`: read the files, validate (`#!lua\n` prefix, no `)lua"`, no `\r`), add to `CMAKE_CONFIGURE_DEPENDS`, `configure_file` into `generated/sink/lua_sources.hpp`
- [x] 2.3 Replace the literals in `lua_scripts.cpp` with `#include "sink/lua_sources.hpp"`

## 3. Tooling and docs

- [x] 3.1 `tools/lint_lua.sh`: remove the embedded-literal branch; require exactly the four files in `src/sink/lua/`
- [x] 3.2 `src/sink/lua/README.md`: update status note and table row, remove the Migration section, keep the SHA table

## 4. Verify

- [x] 4.1 `make lint-lua` lists the 4 files and passes
- [x] 4.2 `make cmake-configure && make build && make test` exit 0
- [x] 4.3 Hash the embedded scripts (from the built binary's strings or a throwaway check) and confirm the SHA1s are unchanged
- [x] 4.4 Edit a comment in a `.lua`, run `make build`, confirm `lua_scripts.cpp` is rebuilt; revert the edit
- [x] 4.5 Negative check: temporarily add `)lua"` to a script, confirm configure fails with a clear message; revert
