#!/usr/bin/env bash

set -euo pipefail

resolve_project_root() {
  local path="$1"

  while [ -L "$path" ]; do
    local resolved
    resolved="$(readlink "$path")"

    if [[ "$resolved" == /* ]]; then
      path="$resolved"
    else
      path="$(cd "$(dirname "$path")" && pwd)/$resolved"
    fi
  done

  local script_dir
  script_dir="$(cd "$(dirname "$path")" && pwd)"
  dirname "$script_dir"
}

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "Missing required command: $name"
    exit 1
  fi
}

stop_running_app() {
  osascript -e 'tell application id "com.sebastian.polar-dash.breathingbar" to quit' >/dev/null 2>&1 || true
  pkill -f "$APP_PATH/Contents/MacOS/BreathingBar" >/dev/null 2>&1 || true
  pkill -f "swift run --package-path .*macos/BreathingBar" >/dev/null 2>&1 || true
  pkill -f "$PACKAGE_PATH/.build/.*/BreathingBar" >/dev/null 2>&1 || true

  local attempts=25
  while pgrep -f "$APP_PATH/Contents/MacOS/BreathingBar" >/dev/null 2>&1 && [ "$attempts" -gt 0 ]; do
    sleep 0.2
    attempts=$((attempts - 1))
  done

  if pgrep -f "$APP_PATH/Contents/MacOS/BreathingBar" >/dev/null 2>&1; then
    echo "BreathingBar did not stop cleanly."
    exit 1
  fi
}

detect_signing_identity() {
  if [[ -n "${BREATHINGBAR_SIGN_IDENTITY:-}" ]]; then
    printf '%s\n' "$BREATHINGBAR_SIGN_IDENTITY"
    return 0
  fi

  local identity
  identity="$(
    security find-identity -v -p codesigning 2>/dev/null |
      awk -F '"' '/Apple Development:/ { print $2; exit }'
  )"

  if [[ -n "$identity" ]]; then
    printf '%s\n' "$identity"
  else
    printf '%s\n' "-"
  fi
}

build_app() {
  swift build --package-path "$PACKAGE_PATH" --configuration "$BUILD_CONFIGURATION"
  BUILD_BIN_DIR="$(swift build --package-path "$PACKAGE_PATH" --configuration "$BUILD_CONFIGURATION" --show-bin-path)"
  EXECUTABLE_PATH="$BUILD_BIN_DIR/BreathingBar"
  RESOURCE_BUNDLE_PATH="$BUILD_BIN_DIR/BreathingBar_BreathingBarCore.bundle"

  if [[ ! -x "$EXECUTABLE_PATH" ]]; then
    echo "Missing built executable: $EXECUTABLE_PATH"
    exit 1
  fi

  if [[ ! -d "$RESOURCE_BUNDLE_PATH" ]]; then
    echo "Missing SwiftPM resource bundle: $RESOURCE_BUNDLE_PATH"
    exit 1
  fi
}

stage_app() {
  mkdir -p "$STAGING_APP/Contents/MacOS" "$STAGING_APP/Contents/Resources"
  cp "$INFO_PLIST_PATH" "$STAGING_APP/Contents/Info.plist"
  ditto "$EXECUTABLE_PATH" "$STAGING_APP/Contents/MacOS/BreathingBar"
  ditto "$RESOURCE_BUNDLE_PATH" "$STAGING_APP/Contents/Resources/$(basename "$RESOURCE_BUNDLE_PATH")"
}

install_app() {
  mkdir -p "$APPLICATIONS_DIR"
  stop_running_app
  rm -rf "$APP_PATH"
  ditto "$STAGING_APP" "$APP_PATH"
  codesign --force --deep --sign "$SIGNING_IDENTITY" --timestamp=none "$APP_PATH"
  codesign --verify --deep --strict "$APP_PATH"
}

launch_app() {
  open "$APP_PATH"

  local attempts=25
  while ! pgrep -f "$APP_PATH/Contents/MacOS/BreathingBar" >/dev/null 2>&1 && [ "$attempts" -gt 0 ]; do
    sleep 0.2
    attempts=$((attempts - 1))
  done

  if ! pgrep -f "$APP_PATH/Contents/MacOS/BreathingBar" >/dev/null 2>&1; then
    echo "BreathingBar failed to launch from $APP_PATH"
    exit 1
  fi
}

require_command swift
require_command ditto
require_command codesign
require_command open
require_command osascript

PROJECT_ROOT="${BREATHINGBAR_PROJECT_ROOT:-$(resolve_project_root "${BASH_SOURCE[0]}")}"
PACKAGE_PATH="$PROJECT_ROOT/macos/BreathingBar"
INFO_PLIST_PATH="$PACKAGE_PATH/AppSupport/Info.plist"
BUILD_CONFIGURATION="${BREATHINGBAR_BUILD_CONFIGURATION:-release}"
APPLICATIONS_DIR="${BREATHINGBAR_APPLICATIONS_DIR:-/Applications}"
APP_PATH="$APPLICATIONS_DIR/BreathingBar.app"
TMP_DIR="$(mktemp -d "${TMPDIR%/}/breathingbar-app.XXXXXX")"
STAGING_APP="$TMP_DIR/BreathingBar.app"
BUILD_BIN_DIR=""
EXECUTABLE_PATH=""
RESOURCE_BUNDLE_PATH=""
SIGNING_IDENTITY="$(detect_signing_identity)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if [[ ! -f "$INFO_PLIST_PATH" ]]; then
  echo "Missing app Info.plist template: $INFO_PLIST_PATH"
  exit 1
fi

build_app
stage_app
install_app
launch_app

echo "Installed BreathingBar to $APP_PATH"
echo "Signing identity: $SIGNING_IDENTITY"
