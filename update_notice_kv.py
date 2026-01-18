import os
import json
import requests
import xxhash
from datetime import datetime, timedelta, timezone
from utils.util import KVTransfer

CF_ACCOUNT_ID = os.environ["CF_ACCOUNT_ID"]
CF_API_TOKEN = os.environ["CF_API_TOKEN"]
CF_KV_NAMESPACE_ID = "8ae8975ae1dc472eac0fc89b0a03c8c7"

key = "prod/index.json"
url = "https://prod-noticeindex.bluearchiveyostar.com/" + key

resp = requests.get(url)
resp.raise_for_status()

text = resp.text
data = text.encode("utf-8")
hash32 = xxhash.xxh32(data, seed=0).intdigest()

obj = json.loads(text)
version_value = f"{obj['LatestClientVersion']}_{hash32}"

date_header = resp.headers.get("Date")
dt_utc = datetime.strptime(date_header, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
dt_bj = dt_utc + timedelta(hours=8)
time_value = dt_bj.strftime("%Y-%m-%d %H:%M:%S")

kv = KVTransfer(
    account_id=CF_ACCOUNT_ID,
    api_token=CF_API_TOKEN
)

kv.put_value(CF_KV_NAMESPACE_ID, "notice/localized/version", version_value)
kv.put_value(CF_KV_NAMESPACE_ID, "notice/localized/time", time_value)