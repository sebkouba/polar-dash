#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${HR_STACK_BIN_DIR:-$HOME/.local/bin}"

mkdir -p "$TARGET_DIR"

ln -sf "$PROJECT_ROOT/scripts/hr-dash-stack" "$TARGET_DIR/hron"
ln -sf "$PROJECT_ROOT/scripts/hr-dash-stack" "$TARGET_DIR/hroff"

chmod +x "$TARGET_DIR/hron" "$TARGET_DIR/hroff"

if [[ ":$PATH:" != *":$TARGET_DIR:"* ]]; then
  echo "Installed hron/hroff to: $TARGET_DIR"
  echo "Add to PATH in your shell startup file:"
  echo "  export PATH=\"$TARGET_DIR:\$PATH\""
  echo "and restart your shell."
else
  echo "Installed hron/hroff to: $TARGET_DIR"
fi
