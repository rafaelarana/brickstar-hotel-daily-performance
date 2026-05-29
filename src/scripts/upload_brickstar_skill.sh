#!/usr/bin/env bash
# Mirror the local Brickstar dashboard style skill (~/.claude/skills/...) to the
# workspace under /Workspace/Shared/skills/ so that Genie Spaces and other
# consumers can reference it via absolute paths.
#
# Usage:
#   ./src/scripts/upload_brickstar_skill.sh [profile]
#
# Default profile: brickstar

set -euo pipefail

PROFILE="${1:-brickstar}"
SRC="${BRICKSTAR_SKILL_SRC:-$HOME/.claude/skills/brickstar-dashboard-style}"
DEST="/Workspace/Shared/skills/brickstar-dashboard-style"

if [[ ! -d "$SRC" ]]; then
  echo "❌ Local skill not found at $SRC"
  echo "   Override via BRICKSTAR_SKILL_SRC=/path/to/skill"
  exit 1
fi

echo "Uploading Brickstar skill from $SRC → $DEST (profile=$PROFILE)"

databricks workspace mkdirs "$DEST"          --profile "$PROFILE" >/dev/null
databricks workspace mkdirs "$DEST/examples" --profile "$PROFILE" >/dev/null

for rel in SKILL.md style-guide.md dashboard-template.lvdash.json examples/kpi-card-snippets.md; do
  if [[ -f "$SRC/$rel" ]]; then
    databricks workspace import "$DEST/$rel" \
      --file "$SRC/$rel" \
      --format AUTO --overwrite --profile "$PROFILE" >/dev/null
    echo "  ✓ $rel"
  else
    echo "  ⚠ skipped $rel (not present locally)"
  fi
done

echo
echo "Done. Verify with:"
echo "  databricks workspace list $DEST --profile $PROFILE"
