import json
import shutil
import logging
from datetime import datetime, timedelta, timezone
from os import path, makedirs, environ
from pathlib import Path

from utils.config import Config
from utils.util import R2Transfer, KVTransfer
from update.regions import JPServer, ServerInfoUrlRewriter, SdkUrlRewriter
from xtractor.apktools import APKTools
from xtractor.bundle import BundleExtractor
from PIL import Image

r2 = R2Transfer(
    account_id=environ.get("CF_ACCOUNT_ID"),
    api_token=environ.get("CF_API_TOKEN"),
    r2_access_key_id=environ.get("R2_ACCESS_KEY_ID"),
    r2_secret_access_key=environ.get("R2_SECRET_ACCESS_KEY")
)

decoded_path = path.join(Config.TEMP_DIR, "decoded_apk")
target_path = path.join(decoded_path, "assets", "bin", "Data")
apk_path = path.join(Config.TEMP_DIR, "蔚蓝档案.apk")

R2_BUCKET = "bluearchive-cafe-download"
R2_REMOTE_KEY = "蔚蓝档案.apk"

jp_server = JPServer()
apk_path_temp = jp_server.download_xapk("https://d.apkpure.net/b/XAPK/com.YostarJP.BlueArchive?version=latest&nc=arm64-v8a&sv=24")

apk_tools = APKTools()
apk_tools.extract_apk_file(apk_path_temp)

ServerInfoUrlRewriter().fast_rewrite(target_path, "yostar-serverinfo.bluearchive.cafe")
#ServerInfoUrlRewriter().fast_rewrite(target_path, "yostar-serverinfo.roundrekt.one")

plist_changes = {
    "User_Tracking_Usage_Description": "如果您允许追踪活动，我们将以此作为参考，以考虑提升游戏质量和用户体验。同时，我们也会将其用于优化通知、广告以及应用内活动等。为了今后服务的改进，如能得到您的协助，我们将不胜感激。另外，即使您拒绝，您仍然可以正常游玩。",
    "Camera_Usage_Description": "为使用部分游戏功能，需要获取相机权限。",
    "Micro_phone_Usage_Description": "为使用部分游戏功能，需要麦克风权限。",
    "Photo_Library_Description": "要保存照片，需要照片访问权限。",
    "Photo_Library_Add_Usage_Description": "要保存照片，需要照片访问权限。",
    "Location_Description": "为使用部分游戏功能，需要获取位置信息。"
}

SdkUrlRewriter().fast_rewrite(target_path, "jp-sdk-api.bluearchive.cafe", plist_changes, True)
SdkUrlRewriter().fast_rewrite(path.join(decoded_path, "assets"), "jp-sdk-api.bluearchive.cafe", plist_changes, False)
#SdkUrlRewriter().fast_rewrite(target_path, "jp-sdk-api.roundrekt.one", plist_changes, True)
#SdkUrlRewriter().fast_rewrite(path.join(decoded_path, "assets"), "jp-sdk-api.roundrekt.one", plist_changes, False)

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

for file_path in config.get('Replace', []):
    dst = path.join(decoded_path, 'assets', file_path)
    makedirs(path.dirname(dst), exist_ok=True)
    if path.exists(file_path):
        shutil.copy2(file_path, dst)

extractor = BundleExtractor()
for asset_item in config.get('Modified', []):
    src_data_dir = path.join('bin', 'Data')
    asset_name = path.splitext(asset_item)[0]
    file_src_path = path.join(src_data_dir, asset_item)
    
    if path.exists(file_src_path):
        ext = asset_item.lower()
        if ext.endswith('.png'):
            img_data = Image.open(file_src_path)
            extractor.modify_and_replace(target_path, asset_name, img_data)
        elif ext.endswith(('.otf', '.ttf')):
            with open(file_src_path, 'rb') as f:
                font_binary = f.read()
            extractor.modify_and_replace(target_path, asset_name, font_binary)
        else:
            with open(file_src_path, 'rb') as f:
                raw_content = f.read()
            extractor.modify_and_replace(target_path, asset_name, raw_content)

apk_tools.build(decoded_path, path.join(Config.TEMP_DIR, "repacked.apk"))
apk_tools.sign(path.join(Config.TEMP_DIR, "repacked.apk"), apk_path)
apk_tools.clean()

r2.upload_file(
    bucket_name=R2_BUCKET,
    local_path=apk_path,
    remote_key=R2_REMOTE_KEY
)

tz_beijing = timezone(timedelta(hours=8))
current_time_beijing = datetime.now(tz_beijing).strftime("%Y-%m-%d %H:%M:%S")

print(f"运行完毕。文件已成功部署至 R2 (北京时间: {current_time_beijing})")

kv = KVTransfer(
    account_id=environ.get("CF_ACCOUNT_ID"),
    api_token=environ.get("CF_API_TOKEN")
)

latest_version = jp_server.get_latest_version() 

kv_namespace = "8ae8975ae1dc472eac0fc89b0a03c8c7"

kv_value = json.dumps({
    "version": latest_version,
    "time": current_time_beijing
}, ensure_ascii=False, separators=(',', ':'))

kv_result = kv.put_value(kv_namespace, "Apk.Localized", kv_value)

print(f"KV部署完毕 (北京时间: {current_time_beijing})")
