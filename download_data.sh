#!/usr/bin/env bash
#
# Download SAM.gov Entity Registration data from Google Drive.
#
# Prerequisites:
#   brew install rclone
#   rclone config create gdrive drive scope=drive
#   (follow browser auth flow)
#
# Usage:
#   ./download_data.sh          # download all data
#   ./download_data.sh --dry-run  # preview what would be downloaded
#

set -euo pipefail

DRIVE_FOLDER_ID="0AL5UY8rhSOEDUk9PVA"
REMOTE_NAME="gdrive"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)/data"

if ! command -v rclone &>/dev/null; then
    echo "Error: rclone is not installed."
    echo "  brew install rclone"
    echo "  rclone config create gdrive drive scope=drive"
    exit 1
fi

if ! rclone listremotes | grep -q "^${REMOTE_NAME}:$"; then
    echo "Error: rclone remote '${REMOTE_NAME}' not configured."
    echo "  rclone config create gdrive drive scope=drive"
    exit 1
fi

EXTRA_ARGS=()
if [[ "${1:-}" == "--dry-run" ]]; then
    EXTRA_ARGS+=(--dry-run)
    echo "Dry run — no files will be downloaded."
fi

echo "Downloading SAM.gov data to ${LOCAL_DIR}/ ..."
rclone copy \
    "${REMOTE_NAME}:sam-opendata" \
    "${LOCAL_DIR}" \
    --drive-root-folder-id="${DRIVE_FOLDER_ID}" \
    --create-empty-src-dirs \
    --progress \
    "${EXTRA_ARGS[@]}"

echo "Done."
