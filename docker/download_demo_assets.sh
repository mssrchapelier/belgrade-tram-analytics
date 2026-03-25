#!/bin/sh

# NOTE: not checking hashsums of files, just that the paths specified exist
fetch_file_if_missing() {
  local DEST_PATH="$1"
  local SRC_URL="$2"

  if [ ! -f "$DEST_PATH" ]; then
    # if the file does not exist at DEST_PATH: download
    # - create the parent directory
    mkdir -p "$(dirname "$DEST_PATH")"
    # - download
    echo "Downloading asset file: ${DEST_PATH} from URL: ${SRC_URL}"
    wget -q "$SRC_URL" -O "$DEST_PATH"
  fi
}

fetch_and_unzip_dir_if_missing() {
  local DEST_DIR="$1"
  local SRC_URL="$2"

  if [ ! -d "$DEST_DIR" ]; then
    # if the directory does not exist at DEST_PATH:
    # - create directory
    mkdir -p "$DEST_DIR"
    # - download zip
    local ZIP_PATH=$(mktemp)
    echo "Downloading archive for asset directory: ${DEST_DIR} from URL: ${SRC_URL}"
    wget -q "$SRC_URL" -O "$ZIP_PATH"
    # - extract
    unzip -q "$ZIP_PATH" -d "$DEST_DIR"
    # - remove the archive
    # TODO: a more robust cleanup, esp. if exits with an error
    rm "$ZIP_PATH"
  fi
}

# TODO: offload to a manifest file (url-path tuples) and generalise processing

# asset URLs and paths
VIDEO_URL="https://pub-16ed5bf3dc1b42ce9bbd3b0e3e072a42.r2.dev/demo/video/demo.mp4"
VIDEO_PATH="${ASSETS_DIR}/video/demo.mp4"

DETECTOR_TRAM_WEIGHTS_URL="https://pub-16ed5bf3dc1b42ce9bbd3b0e3e072a42.r2.dev/demo/weights/yolo-v11n-trams.pt"
DETECTOR_TRAM_WEIGHTS_PATH="${ASSETS_DIR}/weights/yolo-v11n-trams.pt"

DETECTOR_CAR_WEIGHTS_URL="https://pub-16ed5bf3dc1b42ce9bbd3b0e3e072a42.r2.dev/demo/weights/yolo-v11n-cars.pt"
DETECTOR_CAR_WEIGHTS_PATH="${ASSETS_DIR}/weights/yolo-v11n-cars.pt"

CONFIGS_URL="https://pub-16ed5bf3dc1b42ce9bbd3b0e3e072a42.r2.dev/demo/configs.zip"
# Currently: checking only that the config parent directory exists.
# Assuming that if it does, then all of the expected files are in it.
# Listing all expected paths and URLs in a manifest file will enable individual checks.
CONFIGS_PARENT_DIR="${ASSETS_DIR}/config"

fetch_file_if_missing "$VIDEO_PATH" "$VIDEO_URL"
fetch_file_if_missing "$DETECTOR_TRAM_WEIGHTS_PATH" "$DETECTOR_TRAM_WEIGHTS_URL"
fetch_file_if_missing "$DETECTOR_CAR_WEIGHTS_PATH" "$DETECTOR_CAR_WEIGHTS_URL"
fetch_and_unzip_dir_if_missing "$CONFIGS_PARENT_DIR" "$CONFIGS_URL"
