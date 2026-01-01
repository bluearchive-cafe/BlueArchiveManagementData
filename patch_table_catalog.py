import json
import binascii
import subprocess
import os
import sys
import random
import string
from pathlib import Path
from datetime import datetime, timedelta, timezone
from utils.util import R2Transfer, KVTransfer
from os import path, makedirs, environ

def calculate_crc32(file_path):
    with open(file_path, 'rb') as f:
        return binascii.crc32(f.read()) & 0xFFFFFFFF

def serialize_table_catalog():
    print(f"[*] 正在调用 MemoryPackRepacker 序列化 TableCatalog...")
    cmd = ["./other/MemoryPackRepacker", "serialize", "table", "TableCatalog.json", "TableCatalog.bytes"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[+] TableCatalog.bytes 序列化成功")
        return True
    else:
        print(f"[!] 序列化失败: {result.stderr}")
        return False

custom_dir = sys.argv[1] if len(sys.argv) > 1 else "default"
catalog_path = Path("TableCatalog.json")
print(f"[*] 开始处理流程")

if not catalog_path.exists():
    print(f"[!] 错误: 找不到 {catalog_path}")
    sys.exit(1)

with open(catalog_path, "r", encoding="utf8") as f:
    catalog_data = json.load(f)

has_modded = False
target_files = [Path("ExcelDB.db"), Path("Excel.zip")]

print(f"[*] 正在校验本地资源文件...")
for p in [f for f in target_files if f.exists()]:
    item = catalog_data.get("Table", {}).get(p.name)
    if item:
        new_size = p.stat().st_size
        new_crc = calculate_crc32(p)
        print(f"    - {p.name}: CRC={new_crc:08X}, Size={new_size}")
        
        if item.get("size") != new_size or item.get("crc") != new_crc:
            print(f"    [!] 检测到变更: {p.name}")
            item.update({"size": new_size, "crc": new_crc})
            has_modded = True
    else:
        print(f"    [?] 警告: {p.name} 在 TableCatalog 中没有对应条目")

if has_modded:
    print(f"[*] 更新 TableCatalog.json 并生成新的 Hash...")
    with open(catalog_path, "w", encoding="utf8") as f:
        json.dump(catalog_data, f, ensure_ascii=False, indent=2)
    
    random_hash = ''.join(random.choices(string.digits, k=10))
    with open("TableCatalog.hash", "w", encoding="utf8") as f:
        f.write(random_hash)
    print(f"[+] Hash 生成完毕: {random_hash}")
else:
    print(f"[-] 文件无变更，跳过 Catalog 更新阶段")

if serialize_table_catalog():
    tz_beijing = timezone(timedelta(hours=8))
    current_time_beijing = datetime.now(tz_beijing).strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[*] 正在初始化 Cloudflare R2/KV 传输...")
    r2 = R2Transfer(
        account_id=environ.get("CF_ACCOUNT_ID"),
        api_token=environ.get("CF_API_TOKEN"),
        r2_access_key_id=environ.get("R2_ACCESS_KEY_ID"),
        r2_secret_access_key=environ.get("R2_SECRET_ACCESS_KEY")
    )
    R2_BUCKET = "bluearchive-cafe-clientpatch"
    kv = KVTransfer(
        account_id=environ.get("CF_ACCOUNT_ID"),
        api_token=environ.get("CF_API_TOKEN")
    )
    kv_namespace = "8ae8975ae1dc472eac0fc89b0a03c8c7"
    kv_value = json.dumps({
        "version": custom_dir,
        "time": current_time_beijing
    }, ensure_ascii=False, separators=(',', ':'))
    try:

        files_to_upload = ["ExcelDB.db", "Excel.zip", "TableCatalog.bytes"]
        if os.path.exists("TableCatalog.hash"):
            files_to_upload.append("TableCatalog.hash")
            
        print(f"[*] 开始上传文件至 R2 Bucket: {R2_BUCKET}")
        for fname in [f for f in files_to_upload if os.path.exists(f)]:
            remote_path = f"{custom_dir}/{fname}"
            r2.upload_file(
                bucket_name=R2_BUCKET,
                local_path=os.path.abspath(fname),
                remote_key=remote_path
            )
            print(f"    [+] 上传成功: {fname} -> {remote_path}")
            
        print(f"[√] R2 部署完毕 (北京时间: {current_time_beijing})")

        print(f"[*] 正在更新 Cloudflare KV...")
        kv_result = kv.put_value(kv_namespace, "Localization.Localized", kv_value)
        print(f"[√] KV 部署完毕 (北京时间: {current_time_beijing})")

    except Exception as e:
        print(f"[!] 部署过程中出现异常: {str(e)}")
else:
    print(f"[!] 序列化失败，停止后续上传任务")
