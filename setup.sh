#!/usr/bin/env bash
# setup.sh — bootstrap the Brickstar Hotel Daily Performance sample on a
# fresh desktop.
#
# Idempotent. Safe to re-run.
#
# What it does:
#   1. Verifies core tools (bash, curl, git, python3)
#   2. Installs uv (Python package + venv manager) if missing
#   3. Installs the Databricks CLI if missing
#   4. Creates .venv via uv + installs requirements.txt
#   5. Verifies the Databricks CLI profile is configured
#   6. Verifies the target UC catalog is reachable
#   (the SQL warehouse is created by the bundle on first `bundle deploy`)
#
# Usage:
#   ./setup.sh                        # uses defaults below
#   PROFILE=my-profile ./setup.sh
#   CATALOG=my_catalog ./setup.sh
#   ./setup.sh --skip-auth            # skip the profile/catalog checks
#
# Required system tools (must exist before running): bash, curl, git, python3 (>=3.10).

set -euo pipefail

# ──────────────────────────── config ─────────────────────────────
PROFILE="${PROFILE:-brickstar}"
CATALOG="${CATALOG:-classic_stable_89j9qf}"
MIN_CLI_VERSION="0.281.0"
SKIP_AUTH=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-auth)         SKIP_AUTH=true; shift ;;
    --profile)           PROFILE="$2"; shift 2 ;;
    --catalog)           CATALOG="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# ──────────────────────────── helpers ────────────────────────────
say()  { printf "\n\033[1;36m▶ %s\033[0m\n" "$*"; }
ok()   { printf "  \033[32m✓\033[0m %s\n" "$*"; }
warn() { printf "  \033[33m⚠\033[0m %s\n" "$*"; }
err()  { printf "  \033[31m✗\033[0m %s\n" "$*" >&2; }

require_cmd() {
  if command -v "$1" >/dev/null 2>&1; then
    ok "$1 ($(command -v "$1"))"
  else
    err "missing required tool: $1"
    return 1
  fi
}

# ──────────────────────────── 1 · core tools ─────────────────────
say "Checking core tools"
require_cmd bash
require_cmd curl
require_cmd git
require_cmd python3

PY_VER=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
if python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'; then
  ok "python3 $PY_VER"
else
  err "python3 $PY_VER is too old; need >= 3.10"
  exit 1
fi

# ──────────────────────────── 2 · uv ──────────────────────────────
say "Checking uv"
if ! command -v uv >/dev/null 2>&1; then
  warn "uv not found · installing via the official installer"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # uv installs into ~/.local/bin by default
  export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
  if ! command -v uv >/dev/null 2>&1; then
    err "uv install did not put uv on PATH · open a new shell or add ~/.local/bin to PATH"
    exit 1
  fi
fi
ok "uv $(uv --version | awk '{print $2}')"

# ──────────────────────────── 3 · Databricks CLI ──────────────────
say "Checking Databricks CLI"
if ! command -v databricks >/dev/null 2>&1; then
  warn "Databricks CLI not found · installing"
  case "$(uname -s)" in
    Darwin)
      if command -v brew >/dev/null 2>&1; then
        brew install databricks/tap/databricks
      else
        curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      fi
      ;;
    Linux)
      curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      ;;
    *)
      err "unsupported OS for auto-install: $(uname -s) · install Databricks CLI manually"
      exit 1
      ;;
  esac
fi

CLI_VERSION=$(databricks --version | awk '{print $NF}' | sed 's/^v//')
ok "Databricks CLI $CLI_VERSION"

# version check (semver-ish)
if [[ "$(printf '%s\n%s' "$MIN_CLI_VERSION" "$CLI_VERSION" | sort -V | head -1)" != "$MIN_CLI_VERSION" ]]; then
  warn "Databricks CLI $CLI_VERSION is older than recommended $MIN_CLI_VERSION · dashboard dataset_schema may not work"
fi

# ──────────────────────────── 4 · Python venv ─────────────────────
say "Creating .venv with uv"
if [[ ! -d .venv ]]; then
  uv venv
fi
ok ".venv ready ($(.venv/bin/python --version))"

say "Installing Python deps from requirements.txt"
uv pip install -r requirements.txt
ok "deps installed"

if [[ "$SKIP_AUTH" = true ]]; then
  say "Skipping auth + workspace checks (--skip-auth)"
  cat <<EOF

  Next steps:
    source .venv/bin/activate
    databricks bundle validate -t dev
EOF
  exit 0
fi

# ──────────────────────────── 5 · CLI auth ───────────────────────
say "Checking Databricks profile '$PROFILE'"
if databricks current-user me --profile "$PROFILE" >/dev/null 2>&1; then
  USER_INFO=$(databricks current-user me --profile "$PROFILE" --output json)
  USER_EMAIL=$(printf '%s' "$USER_INFO" | python3 -c "import json,sys; print(json.load(sys.stdin)['emails'][0]['value'])")
  HOST=$(databricks auth describe --profile "$PROFILE" 2>/dev/null | awk '/^Host:/{print $2}')
  ok "authenticated as $USER_EMAIL @ $HOST"
else
  err "profile '$PROFILE' is not configured (or token expired)"
  cat <<EOF >&2

  Configure it once:
    databricks auth login --host https://your-workspace.azuredatabricks.net --profile $PROFILE

  Then re-run ./setup.sh
EOF
  exit 1
fi

# ──────────────────────────── 6 · catalog ────────────────────────
say "Verifying UC catalog '$CATALOG'"
if databricks catalogs get "$CATALOG" --profile "$PROFILE" >/dev/null 2>&1; then
  ok "catalog $CATALOG exists"
else
  err "catalog $CATALOG not found or no permission"
  cat <<EOF >&2

  Create or grant access:
    databricks catalogs create $CATALOG --profile $PROFILE
    # or pass a different one via CATALOG=my_catalog ./setup.sh
EOF
  exit 1
fi

# ──────────────────────────── done ───────────────────────────────
say "Setup complete!"
cat <<EOF

  Activate the venv:
    source .venv/bin/activate

  Validate + deploy + run the bundle:
    databricks bundle validate -t dev --profile $PROFILE
    databricks bundle deploy   -t dev --profile $PROFILE --auto-approve
    databricks bundle run brickstar_validation_pipeline -t dev --profile $PROFILE

  Post-deploy:
    ./src/scripts/upload_brickstar_skill.sh $PROFILE        # optional
    python src/scripts/create_genie_space.py --profile $PROFILE

EOF
