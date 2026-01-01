set -euo pipefail

REGION="${1:-JP}"
SERVER_ID_LOWER=$(echo "$REGION" | tr '[:upper:]' '[:lower:]')
INFO_FILE="./info_${SERVER_ID_LOWER}.json"

ADDRESSABLE_CATALOG_URL=$(jq -r ".AddressableCatalogUrl" "$INFO_FILE")

if [ "$ADDRESSABLE_CATALOG_URL" = "null" ] || [ -z "$ADDRESSABLE_CATALOG_URL" ]; then
  echo "Error: AddressableCatalogUrl not found in $INFO_FILE"
  exit 1
fi

download_file() {
  local url=$1
  local output=$2
  local max_retries=3
  local retry_count=0
  mkdir -p "$(dirname "$output")"

  while [ $retry_count -lt $max_retries ]; do
    echo "Downloading: $url (Attempt $((retry_count+1))/$max_retries)"
    http_status=$(curl -sL -w "%{http_code}" -o "$output" "$url")
    if [ "$http_status" -eq 200 ] && [ -s "$output" ]; then
      echo "Download successful: $output"
      return 0
    else
      echo "Download failed (HTTP $http_status): $url"
      rm -f "$output"
      retry_count=$((retry_count+1))
      sleep 2
    fi
  done
  return 1
}

TABLE_DOWNLOADS=()

if [ "$REGION" = "CN" ]; then
  TABLE_VERSION=$(jq -r ".TableVersion" "$INFO_FILE")
  if [ "$TABLE_VERSION" = "null" ]; then echo "Error: TableVersion missing"; exit 1; fi
  
  MANIFEST_URL="${ADDRESSABLE_CATALOG_URL}/Manifest/TableBundles/${TABLE_VERSION}/TableManifest"
  MANIFEST_OUTPUT="./TableManifest.json"
  
  download_file "$MANIFEST_URL" "$MANIFEST_OUTPUT"
  
  EXCEL_CRC=$(jq -r '.Table["Excel.zip"].Crc' "$MANIFEST_OUTPUT")
  EXCELDB_CRC=$(jq -r '.Table["ExcelDB.db"].Crc' "$MANIFEST_OUTPUT")
  
  TABLE_DOWNLOADS=(
    "${ADDRESSABLE_CATALOG_URL}/pool/TableBundles/${EXCEL_CRC:0:2}/$EXCEL_CRC|./TableBundles/Excel.zip"
    "${ADDRESSABLE_CATALOG_URL}/pool/TableBundles/${EXCELDB_CRC:0:2}/$EXCELDB_CRC|./TableBundles/ExcelDB.db"
  )
elif [ "$REGION" = "GL" ]; then
  BASE="${ADDRESSABLE_CATALOG_URL%/}/Preload/TableBundles"
  TABLE_DOWNLOADS=(
    "${BASE}/Excel.zip|./TableBundles/Excel.zip"
    "${BASE}/ExcelDB.db|./TableBundles/ExcelDB.db"
  )
else
  BASE="${ADDRESSABLE_CATALOG_URL%/}/TableBundles"
  TABLE_DOWNLOADS=(
    "${BASE}/Excel.zip|./TableBundles/Excel.zip"
    "${BASE}/ExcelDB.db|./TableBundles/ExcelDB.db"
    "${BASE}/TableCatalog.bytes|./TableBundles/TableCatalog.bytes"
  )
fi

for item in "${TABLE_DOWNLOADS[@]}"; do
  IFS='|' read -r url output <<< "$item"
  download_file "$url" "$output" || exit 1
done
