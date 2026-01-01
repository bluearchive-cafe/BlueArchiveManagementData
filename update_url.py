import json
import os
from pathlib import Path
from os import path
from update.regions import CNServer, GLServer, JPServer, BaseServer
from utils.config import Config
from lib.console import notice

def load_existing_data(server_id):
    file_path = f"info_{server_id.lower()}.json"
    if path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_server_data(server_id, data):
    file_name = f"info_{server_id.lower()}.json"
    if server_id == "CN":
        output = {
            "ServerInfoDataUrl": data.get("ServerInfoDataUrl", ""),
            "AddressableCatalogUrl": data.get("AddressableCatalogUrl", ""),
            "GameVersion": data.get("GameVersion", ""),
            "TableVersion": data.get("TableVersion", ""),
            "MediaVersion": data.get("MediaVersion", ""),
            "ResourceVersion": data.get("ResourceVersion", "")
        }
    else:
        output = {
            "ServerInfoDataUrl": data.get("ServerInfoDataUrl", ""),
            "AddressableCatalogUrl": data.get("AddressableCatalogUrl", ""),
            "GameVersion": data.get("GameVersion", "")
        }
    with open(file_name, "w", encoding="utf-8") as json_file:
        json.dump(output, json_file, indent=4, ensure_ascii=False)

def set_github_output(name, value):
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"{name}={value}\n")

def update_jp():
    jp_server = JPServer()
    latest = jp_server.get_latest_version()
    existing_data = load_existing_data("JP")
    current = existing_data.get("GameVersion")
    old_catalog = existing_data.get("AddressableCatalogUrl", "")
    is_version_changed = "false"
    
    data = existing_data.copy()

    if current != latest:
        is_version_changed = "true"
        notice(f"[JP] 检测到新版本: {current} -> {latest}")
        apk_path = jp_server.download_xapk("https://d.apkpure.net/b/XAPK/com.YostarJP.BlueArchive?version=latest&nc=arm64-v8a&sv=24")
        if apk_path and path.exists(apk_path):
            BaseServer().extract_apks_file(apk_path)
            data.update({"GameVersion": latest})
        url = jp_server.get_server_url()
        data.update({
            "ServerInfoDataUrl": url, 
            "AddressableCatalogUrl": jp_server.get_addressable_catalog_url(url)
        })
    else:
        notice(f"[JP] 未检测到新版本: {current}")
        url = data.get("ServerInfoDataUrl", "")
        if url:
            data.update({
                "AddressableCatalogUrl": jp_server.get_addressable_catalog_url(url)
            })

    new_catalog = data.get("AddressableCatalogUrl", "")
    is_catalog_changed = "true" if old_catalog != new_catalog else "false"

    save_server_data("JP", data)
    
    jp_url = data.get("ServerInfoDataUrl", "")
    jp_catalog = data.get("AddressableCatalogUrl", "")
    
    set_github_output("jp_version_changed", is_version_changed)
    set_github_output("jp_catalog_changed", is_catalog_changed)
    set_github_output("jp_version", latest if latest else "")
    set_github_output("jp_server_info_name", path.splitext(jp_url.split('/')[-1])[0] if jp_url else "")
    set_github_output("jp_addressable_name", jp_catalog.split('/')[-1] if jp_catalog else "")
    
    notice("[JP] 部署完毕")

def update_gl():
    gl_server = GLServer()
    latest = gl_server.get_latest_version()
    existing_data = load_existing_data("GL")
    current = existing_data.get("GameVersion")
    old_catalog = existing_data.get("AddressableCatalogUrl", "")
    is_version_changed = "false"

    url = gl_server.get_server_url(latest)
    new_catalog = f"{url.rsplit('/', 1)[0]}/"
    
    data = {
        "ServerInfoDataUrl": url,
        "AddressableCatalogUrl": new_catalog,
        "GameVersion": current
    }

    if current != latest:
        is_version_changed = "true"
        notice(f"[GL] 检测到新版本: {current} -> {latest}")
        data.update({"GameVersion": latest})
    else:
        notice(f"[GL] 未检测到新版本: {current}")
    
    is_catalog_changed = "true" if old_catalog != new_catalog else "false"
    
    save_server_data("GL", data)
    set_github_output("gl_version_changed", is_version_changed)
    set_github_output("gl_catalog_changed", is_catalog_changed)
    set_github_output("gl_version", latest if latest else "")
    notice("[GL] 部署完毕")

def update_cn():
    cn_server = CNServer()
    latest = cn_server.get_latest_version()
    existing_data = load_existing_data("CN")
    current = existing_data.get("GameVersion")
    is_version_changed = "false"
    
    info = cn_server.get_server_info(latest)
    
    new_table = info.get("TableVersion", "")
    new_media = info.get("MediaVersion", "")
    new_res = info.get("ResourceVersion", "")
    
    is_table_changed = "true" if str(existing_data.get("TableVersion")) != str(new_table) else "false"
    is_media_changed = "true" if str(existing_data.get("MediaVersion")) != str(new_media) else "false"
    is_res_changed = "true" if str(existing_data.get("ResourceVersion")) != str(new_res) else "false"

    data = {
        "ServerInfoDataUrl": cn_server.urls["info"],
        "AddressableCatalogUrl": info["AddressablesCatalogUrlRoots"][0],
        "GameVersion": current,
        "TableVersion": new_table,
        "MediaVersion": new_media,
        "ResourceVersion": new_res
    }

    if current != latest:
        is_version_changed = "true"
        notice(f"[CN] 检测到新版本: {current} -> {latest}")
        data.update({"GameVersion": latest})
    else:
        notice(f"[CN] 未检测到新版本: {current}")
    
    save_server_data("CN", data)
    set_github_output("cn_version_changed", is_version_changed)
    set_github_output("cn_version", latest if latest else "")
    set_github_output("cn_table_changed", is_table_changed)
    set_github_output("cn_media_changed", is_media_changed)
    set_github_output("cn_res_changed", is_res_changed)
    notice("[CN] 部署完毕")

def main():
    tasks = [
        ("JP", update_jp),
        ("GL", update_gl),
        ("CN", update_cn)
    ]
    for name, task in tasks:
        try:
            task()
        except Exception as e:
            print(f"[!] {name} failed: {e}")

if __name__ == "__main__":
    main()
