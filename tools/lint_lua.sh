#!/usr/bin/env bash
# Lints the Redis Lua scripts: evm_listener/src/sink/lua/*.lua (exactly the 4 Redis scripts) and any other *.lua in
# the repo. CMake embeds the Redis scripts into the binary at build time (evm_listener/CMakeLists.txt).
#
# Checks per script:
#   1. luacheck with the repo .luacheckrc (exact Redis sandbox globals; catches typos like ARVG, print, os.time)
#   2. luac5.1 -p (real Lua 5.1 parser; luacheck accepts newer syntax such as `//` that Redis rejects)
#   3. Redis invariants: '#!lua' first line without allow-oom, LF line endings, no UTF-8 BOM, no ')lua"'
#
# Usage: tools/lint_lua.sh
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

for tool in luacheck luac5.1 python3; do
  if ! command -v "$tool" > /dev/null 2>&1; then
    echo "error: $tool is not installed or not in PATH. Run inside the dev container (make shell)." >&2
    exit 1
  fi
done

luacheckrc="$repo_root/.luacheckrc"
if [ ! -f "$luacheckrc" ]; then
  echo "error: $luacheckrc not found; it defines the Redis sandbox globals." >&2
  exit 1
fi

echo "==> Linting Lua scripts ($(luacheck --version | head -n 1))..."

python3 - "$luacheckrc" << 'EOF'
import pathlib
import re
import subprocess
import sys

LUACHECKRC = sys.argv[1]
REDIS_LUA_DIR = pathlib.Path("evm_listener/src/sink/lua")
EXPECTED_REDIS_SCRIPTS = {"emit.lua", "ckpt.lua", "final_trim.lua", "lease.lua"}
SHEBANG_RE = re.compile(r"#!lua(?:[ \t]+([^\n]*))?\n")

# (label, source, path for diagnostics, path line of Lua line 1, is_redis_script)
scripts = []

# 1. Standalone .lua files (tracked + untracked non-ignored)
listed = subprocess.run(
    ["git", "ls-files", "--cached", "--others", "--exclude-standard", "*.lua"],
    text=True, capture_output=True, check=True,
).stdout.split("\n")
for name in sorted(set(filter(None, listed))):
    path = pathlib.Path(name)
    if not path.is_file():
        continue  # deleted in the working tree
    raw = path.read_bytes()
    try:
        src = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        print(f"FAILED {path}: not valid UTF-8 ({e})", file=sys.stderr)
        sys.exit(1)
    scripts.append((str(path), src, str(path), 1, path.parent == REDIS_LUA_DIR))

redis_count = sum(1 for s in scripts if s[4])
found = {pathlib.Path(s[2]).name for s in scripts if s[4]}
if found != EXPECTED_REDIS_SCRIPTS:
    print(
        f"Error: {REDIS_LUA_DIR}/ must hold exactly {sorted(EXPECTED_REDIS_SCRIPTS)}, found {sorted(found)}.",
        file=sys.stderr,
    )
    sys.exit(1)


def remap(text, label, path, base):
    # luacheck/luac report '<label>:L[:C]'; rewrite to the real file line so editors can jump to it
    fname = label if label.endswith(".lua") else f"{label}.lua"
    return re.sub(
        rf"{re.escape(fname)}:(\d+)",
        lambda m: f"{path}:{base + int(m.group(1)) - 1}",
        text,
    )


def invariant_errors(src):
    errs = []
    if src.startswith("\ufeff"):
        errs.append("starts with a UTF-8 BOM; '#!lua' must be the very first bytes")
    if "\r" in src:
        errs.append("contains CR characters; use LF line endings")
    shebang = SHEBANG_RE.match(src)
    if not shebang:
        errs.append("missing mandatory '#!lua' first line (Redis refuses the whole script up front on OOM only with it)")
    elif "allow-oom" in (shebang.group(1) or ""):
        errs.append("'allow-oom' flag is forbidden in the shebang")
    if ')lua"' in src:
        errs.append("contains ')lua\"', which terminates the C++ raw-string literal it is embedded in")
    return errs


failures = 0
for label, src, path, base, is_redis in scripts:
    errs = invariant_errors(src) if is_redis else []
    fname = label if label.endswith(".lua") else f"{label}.lua"

    lc = subprocess.run(
        ["luacheck", "--config", LUACHECKRC, "--formatter", "plain", "-", "--filename", fname],
        input=src, text=True, errors="replace", capture_output=True,
    )
    if lc.returncode != 0:
        errs.append("luacheck:\n" + remap((lc.stdout + lc.stderr).strip(), label, path, base))

    # errors="replace": luac echoes the offending byte (e.g. half a BOM), which is not valid UTF-8
    lp = subprocess.run(["luac5.1", "-p", "-"], input=src, text=True, errors="replace", capture_output=True)
    if lp.returncode != 0:
        # luac names stdin 'stdin'; label it so remap() can point at the real line
        msg = lp.stderr.strip().replace("luac5.1: stdin:", f"{fname}:")
        errs.append("luac5.1 (Lua 5.1 parser):\n" + remap(msg, label, path, base))

    where = f"{path}:{base}"
    if errs:
        failures += 1
        print(f"FAILED {label} ({where}):", file=sys.stderr)
        for e in errs:
            print("  - " + e.replace("\n", "\n    "), file=sys.stderr)
    else:
        kind = "redis" if is_redis else "lua"
        print(f"  ✓ {label} ({where}, {len(src.splitlines())} lines, {kind}) OK")

if failures:
    print(f"==> {failures} of {len(scripts)} Lua script(s) failed.", file=sys.stderr)
    sys.exit(1)

print(f"==> All {len(scripts)} Lua script(s) passed ({redis_count} Redis scripts).")
EOF
