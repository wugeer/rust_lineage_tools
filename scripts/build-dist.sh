#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TARGETS=${TARGETS:-"x86_64-unknown-linux-musl"}
DIST_DIR="$ROOT_DIR/dist"
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

VERSION="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["packages"][0]["version"])')"

for TARGET in $TARGETS; do
    echo "==> Building target: $TARGET"

    if [[ "$TARGET" == *"musl"* ]]; then
        export PKG_CONFIG_ALL_STATIC=1
    else
        unset PKG_CONFIG_ALL_STATIC || true
    fi

    cargo build --release --target "$TARGET"

    BIN_NAME="hive_lineage"
    if [[ "$TARGET" == *"windows"* ]]; then
        BIN_NAME="hive_lineage.exe"
    fi

    BIN_PATH="$ROOT_DIR/target/$TARGET/release/$BIN_NAME"
    if [[ ! -f "$BIN_PATH" ]]; then
        echo "Failed to find built binary at $BIN_PATH" >&2
        exit 1
    fi

    STAGE_DIR="$DIST_DIR/hive_lineage-v${VERSION}-${TARGET}"
    rm -rf "$STAGE_DIR"
    mkdir -p "$STAGE_DIR"

    cp "$BIN_PATH" "$STAGE_DIR/"
    if command -v strip >/dev/null 2>&1; then
        strip "$STAGE_DIR/$BIN_NAME" || true
    fi
    cp README.md "$STAGE_DIR/README.md"
    cp config.toml "$STAGE_DIR/config.example.toml"

    pushd "$DIST_DIR" >/dev/null
    if [[ "$TARGET" == *"windows"* ]]; then
        ZIP_NAME="hive_lineage-v${VERSION}-${TARGET}.zip"
        rm -f "$ZIP_NAME"
        zip -r "$ZIP_NAME" "hive_lineage-v${VERSION}-${TARGET}" >/dev/null
    else
        TAR_NAME="hive_lineage-v${VERSION}-${TARGET}.tar.gz"
        rm -f "$TAR_NAME"
        tar czf "$TAR_NAME" "hive_lineage-v${VERSION}-${TARGET}"
    fi
    popd >/dev/null
done

echo "Artifacts are available under $DIST_DIR:"
ls -1 "$DIST_DIR"
