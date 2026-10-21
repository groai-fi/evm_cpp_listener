# Spec Delta

## ADDED Requirements

### Requirement: Redis Lua Scripts Embedded From Standalone Files
The Redis Lua scripts SHALL be maintained as standalone `.lua` files under `evm_listener/src/sink/lua/` and embedded into the binary at build time through a CMake-generated header. The binary SHALL NOT load script files at runtime. CMake SHALL reject at configure time any script that does not begin with `#!lua` followed by LF, that contains `)lua"`, or that contains a CR character.

#### Scenario: Editing a script rebuilds the binary
- **WHEN** a `.lua` file in `evm_listener/src/sink/lua/` is modified and `make build` is executed
- **THEN** CMake re-runs configure, regenerates the header, and rebuilds the binary containing the modified script.

#### Scenario: Invalid script rejected at configure time
- **WHEN** a script lacks the `#!lua` first line, contains `)lua"`, or contains a CR character
- **THEN** CMake configure fails with an error naming the file and the violated rule.

#### Scenario: Script bytes are unchanged by the move
- **WHEN** the embedded scripts are hashed with SHA1
- **THEN** the hashes equal those of the files in `evm_listener/src/sink/lua/`, so the `EVALSHA` hashes are stable across the move.
