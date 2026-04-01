#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCHER="$SCRIPT_DIR/stackpulse"
TARGET_DIR="${HOME}/.local/bin"
TARGET_LINK="$TARGET_DIR/stackpulse"

if [[ ! -f "$LAUNCHER" ]]; then
  echo "Launcher not found: $LAUNCHER"
  exit 1
fi

mkdir -p "$TARGET_DIR"
chmod +x "$LAUNCHER"
ln -sfn "$LAUNCHER" "$TARGET_LINK"

echo "Installed launcher symlink: $TARGET_LINK -> $LAUNCHER"

case ":$PATH:" in
  *":$TARGET_DIR:"*)
    echo "PATH already contains $TARGET_DIR"
    ;;
  *)
    echo "Add this line to your shell profile (~/.bashrc or ~/.zshrc):"
    echo "  export PATH=\"$TARGET_DIR:\$PATH\""
    echo "Then run: source ~/.bashrc   # or source ~/.zshrc"
    ;;
esac

echo "Done. You can now run: stackpulse --help"
