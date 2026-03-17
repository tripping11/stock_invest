#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "scripts/bootstrap_macos.sh is for macOS only." >&2
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required. Install it from https://brew.sh" >&2
  exit 1
fi

if ! command -v python3.11 >/dev/null 2>&1; then
  echo "python3.11 not found. Install it with: brew install python@3.11" >&2
  exit 1
fi

if [[ -d ".venv" && ! -x ".venv/bin/python" ]]; then
  backup_dir=".venv.incompatible.$(date +%Y%m%d%H%M%S)"
  mv ".venv" "$backup_dir"
  echo "Moved incompatible .venv to $backup_dir"
fi

python3.11 -m venv .venv
.venv/bin/python -m pip install --upgrade pip setuptools wheel
.venv/bin/python -m pip install -r requirements.txt

cat <<'EOF'
Bootstrap complete.

Run:
  .venv/bin/python scripts/run_quant_scan.py A-share --limit 24 --base-dir /tmp/a_quant_run
  .venv/bin/python scripts/run_quant_deep_dive.py 600328 中盐化工 --skip-tier0 --base-dir /tmp/a_quant_run

Test:
  .venv/bin/python -m unittest discover .agents/skills/shared/tests -v
EOF
