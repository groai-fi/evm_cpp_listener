#!/usr/bin/env bash
# Fails (exit 1) if any secret VALUE found in a local .env* file appears inside a tracked or
# untracked (non-ignored) repository file. Values are never printed; only the key name and file.
# Portable: works with macOS bash 3.2 and the devcontainer's bash 5.
#
# Usage: tools/check_secrets.sh            (run from anywhere inside the repo)
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# Values shorter than this are skipped (ports, flags, "true", ...).
min_len=12

env_list="$(mktemp)"
scan_list="$(mktemp)"
trap 'rm -f "$env_list" "$scan_list"' EXIT

# .env* files that hold secrets: ignored/untracked/tracked, excluding the placeholder-only .env.example.
git ls-files --cached --others --ignored --exclude-standard \
  | grep -E '(^|/)\.env(\.[A-Za-z0-9_.-]+)?$' | grep -v -E '\.env\.example$' > "$env_list" || true
# Also ignored .env files (git ls-files --ignored needs --others): list them explicitly.
git ls-files --others --ignored --exclude-standard \
  | grep -E '(^|/)\.env(\.[A-Za-z0-9_.-]+)?$' | grep -v -E '\.env\.example$' >> "$env_list" || true
sort -u "$env_list" -o "$env_list"

# Files that could be committed: tracked + untracked-but-not-ignored, minus .env* themselves.
git ls-files --cached --others --exclude-standard \
  | grep -v -E '(^|/)\.env(\.[A-Za-z0-9_.-]+)?$' > "$scan_list" || true

fail=0
checked=0
env_count=0
while IFS= read -r env_file; do
  [ -f "$env_file" ] || continue
  env_count=$((env_count + 1))
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) continue ;; esac
    key="${line%%=*}"
    value="${line#*=}"
    value="${value%\"}"; value="${value#\"}"; value="${value%\'}"; value="${value#\'}"
    [ "${#value}" -ge "$min_len" ] || continue
    checked=$((checked + 1))
    while IFS= read -r f; do
      [ -f "$f" ] || continue
      if grep -F -q -- "$value" "$f" 2>/dev/null; then
        echo "LEAK: value of $key (from $env_file) found in $f" >&2
        fail=1
      fi
    done < "$scan_list"
  done < "$env_file"
done < "$env_list"

if [ "$fail" -ne 0 ]; then
  echo "check_secrets: FAILED" >&2
  exit 1
fi
echo "check_secrets: OK (${checked} secret values checked across ${env_count} env file(s), 0 matches)"
