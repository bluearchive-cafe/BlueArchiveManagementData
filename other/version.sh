set -euo pipefail

REGION="${1:-JP}"
INFO_JSON_FILE="${2:-info_jp.json}"

last_seg() { basename "$1"; }

parse_json() {
    local key="$1"
    local file="$2"
    sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$file" | tr -d '\r'
}

ADDRESSABLE_CATALOG_URL=$(parse_json "AddressableCatalogUrl" "$INFO_JSON_FILE")
GAME_VERSION=$(parse_json "GameVersion" "$INFO_JSON_FILE")
TABLE_VERSION=$(parse_json "TableVersion" "$INFO_JSON_FILE")

case "$REGION" in
    "CN")
        BUILD_VERSION="${GAME_VERSION}(${TABLE_VERSION})"
        ;;
    "GL")
        BUILD_VERSION="${GAME_VERSION}($(last_seg "$ADDRESSABLE_CATALOG_URL"))"
        ;;
    *)
        BUILD_VERSION="${GAME_VERSION}($(last_seg "$ADDRESSABLE_CATALOG_URL"))"
        ;;
esac

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "BA_VERSION_NAME=$BUILD_VERSION" >> "$GITHUB_OUTPUT"
    echo "FLATDATA_VERSION_NAME=$GAME_VERSION" >> "$GITHUB_OUTPUT"
    echo "${REGION}_BA_VERSION_NAME=$BUILD_VERSION" >> "$GITHUB_OUTPUT"
    echo "${REGION}_FLATDATA_VERSION_NAME=$GAME_VERSION" >> "$GITHUB_OUTPUT"
else
    echo "BA_VERSION_NAME=$BUILD_VERSION" >> "$GITHUB_OUTPUT"
    echo "FLATDATA_VERSION_NAME=$GAME_VERSION" >> "$GITHUB_OUTPUT"
    echo "${REGION}_BA_VERSION_NAME=$BUILD_VERSION" >> "$GITHUB_OUTPUT"
    echo "${REGION}_FLATDATA_VERSION_NAME=$GAME_VERSION" >> "$GITHUB_OUTPUT"
fi
