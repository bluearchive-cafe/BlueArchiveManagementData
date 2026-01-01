import base64
import json
import os
import re
import struct
import requests
import UnityPy
import stat
import glob
import zlib
import json

from io import BytesIO
from os import path
from typing import Any, Literal, Optional
from cloudscraper import create_scraper
import subprocess
from lib.console import ProgressBar, notice
from lib.downloader import FileDownloader
from lib.encryption import convert_string, create_key, encrypt_string
from lib.structure import JPResource, Resource, CNResource, GLResource
from utils.config import Config
from utils.util import ZipUtils, CRCManip, TaskManager
from xtractor.bundle import BundleExtractor
from urllib.parse import urlparse, urljoin
from threading import Lock
from datetime import datetime, timedelta, timezone

class BaseServer:
    APK_EXTRACT_FOLDER = path.join(Config.TEMP_DIR, "Data")

    def _shared_download(self, apk_url: str, filename: str = None) -> tuple:
        os.makedirs(Config.TEMP_DIR, exist_ok=True)

        downloader = FileDownloader(apk_url, request_method="get", use_cloud_scraper=True, verbose=True)
        apk_data = downloader.get_response(True)
        
        if not apk_data:
            raise LookupError("Cannot fetch apk info.")

        last_modified_beijing = ""
        last_modified_raw = apk_data.headers.get("Last-Modified")
        if last_modified_raw:
            try:
                dt_utc = datetime.strptime(last_modified_raw, "%a, %d %b %Y %H:%M:%S %Z")
                dt_beijing = dt_utc.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                last_modified_beijing = dt_beijing.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass

        if not filename:
            try:
                filename = apk_data.headers["Content-Disposition"].rsplit('"', 2)[-2].encode("ISO8859-1").decode()
            except:
                filename = "download.apk"

        apk_path = path.join(Config.TEMP_DIR, filename).replace("\\", "/")

        if path.exists(apk_path):
            apk_size = int(apk_data.headers.get("Content-Length", 0))
            if apk_size == 0 or path.getsize(apk_path) == apk_size:
                return apk_path, last_modified_beijing

        notice(f"Downloading package: {filename}")
        FileDownloader(apk_url, request_method="get", enable_progress=True, use_cloud_scraper=True).save_file(apk_path)
        return apk_path

    def extract_apks_file(self, apk_path: str, apk_extract_path: str = path.join(Config.TEMP_DIR, "Data")) -> None:
        apk_files = ZipUtils.extract_zip(apk_path, path.join(Config.TEMP_DIR), keywords=["apk"])
        ZipUtils.extract_zip(apk_files, apk_extract_path, zips_dir=Config.TEMP_DIR)

    def extract_apk_file(self, apk_path: str, apk_extract_path: str = path.join(Config.TEMP_DIR, "Data")) -> None:
        ZipUtils.extract_zip(apk_path, apk_extract_path)

class JPServer(BaseServer):
    def download_xapk(self, apk_url: str) -> tuple:
        return self._shared_download(apk_url)

    def get_apk_url(self, version: str) -> str:
        return Config.JP_APKPURE_URL + f"?versionCode={version.split('.')[-1]}"

    def get_latest_version(self) -> str:
        downloader = FileDownloader("https://d.apkpure.net/b/XAPK/com.YostarJP.BlueArchive?version=latest&nc=arm64-v8a&sv=24", request_method="get", use_cloud_scraper=True, verbose=True)
        apk_data = downloader.get_response(True)

        if not apk_data or "Content-Disposition" not in apk_data.headers:
            raise LookupError("Cannot access APKPure or header is missing.")

        try:
            filename = apk_data.headers["Content-Disposition"].rsplit('"', 2)[-2].encode("ISO8859-1").decode()
            
            if version_match := re.search(r"_([\d.]+)_APKPure", filename):
                return version_match.group(1)
                
        except (IndexError, UnicodeDecodeError):
            raise LookupError("Failed to decode filename from headers.")

        raise LookupError("Unable to extract version from APKPure filename.")


    def get_resource_manifest(self, server_url: str) -> Resource:
        resources = JPResource()
        api = FileDownloader(server_url).get_response()
        if not api: raise LookupError("Access failed.")

        base_url = api.json()["ConnectionGroups"][0]["OverrideConnectionGroups"][-1]["AddressablesCatalogUrlRoot"] + "/"
        resources.set_url_link(base_url, "Android/", "MediaResources/", "TableBundles/")

        for item in [("TableCatalog.bytes", "table", resources.table_url), ("Catalog/MediaCatalog.bytes", "media", resources.media_url)]:
            if data := FileDownloader(urljoin(item[2], item[0])).get_bytes():
                JPCatalogDecoder.decode_to_manifest(data, resources, item[1])
        
        if (bundle_data := FileDownloader(urljoin(resources.bundle_url, "bundleDownloadInfo.json")).get_response()) and bundle_data.headers.get("Content-Type") == "application/json":
            for b in bundle_data.json()["BundleFiles"]:
                resources.add_bundle_resource(b["Name"], b["Size"], b["Crc"], b["IsPrologue"], b["IsSplitDownload"])
        return resources.to_resource()

    def get_server_url(self) -> str:
        extractor = BundleExtractor()
        url = version = ""
        for dir, _, files in os.walk(path.join(self.APK_EXTRACT_FOLDER, "assets", "bin", "Data")):
            for file in files:
                fpath = path.join(dir, file)
                if not url and (url_obj := extractor.search_unity_pack(fpath, ["TextAsset"], ["GameMainConfig"], True)):
                    url = self.__decode_server_url(url_obj[0].read().m_Script.encode("utf-8", "surrogateescape"))
                if not version and (version_obj := extractor.search_unity_pack(fpath, ["PlayerSettings"])):
                    version = version_obj[0].read().bundleVersion
                if url and version: break
        return url

    def __decode_server_url(self, data: bytes) -> str:
        json_str = convert_string(base64.b64encode(data).decode(), create_key("GameMainConfig"))
        encrypted_url = json.loads(json_str)["X04YXBFqd3ZpTg9cKmpvdmpOElwnamB2eE4cXDZqc3ZgTg=="]
        return convert_string(encrypted_url, create_key("ServerInfoDataUrl"))

    def get_addressable_catalog_url(self, server_url: str) -> str:
        response = requests.get(server_url)
        data = response.json()
        connection_groups = data.get("ConnectionGroups", [])
        override_groups = connection_groups[0].get("OverrideConnectionGroups", [])
        latest_catalog_url = override_groups[-1].get("AddressablesCatalogUrlRoot")
        return latest_catalog_url

class GLServer(BaseServer):
    def download_xapk(self, apk_url: str) -> str:
        return self._shared_download(apk_url)

    def get_apk_url(self, version: str) -> str:
        return Config.GL_APKPURE_URL + f"?versionCode={version.split('.')[-1]}"

    def get_latest_version(self) -> str:
        downloader = FileDownloader("https://d.apkpure.net/b/XAPK/com.nexon.bluearchive?version=latest&nc=arm64-v8a&sv=24", request_method="get", use_cloud_scraper=True, verbose=True)
        apk_data = downloader.get_response(True)

        if not apk_data or "Content-Disposition" not in apk_data.headers:
            raise LookupError("Cannot access APKPure or header is missing.")

        try:
            filename = apk_data.headers["Content-Disposition"].rsplit('"', 2)[-2].encode("ISO8859-1").decode()
            
            if version_match := re.search(r"_([\d.]+)_APKPure", filename):
                return version_match.group(1)
                
        except (IndexError, UnicodeDecodeError):
            raise LookupError("Failed to decode filename from headers.")

        raise LookupError("Unable to extract version from APKPure filename.")

    def get_resource_manifest(self, server_url: str) -> Resource:
        resources = GLResource()
        resources.set_url_link(server_url.rsplit("/", 1)[0] + "/")
        data = FileDownloader(server_url).get_response()
        if not data: raise LookupError("Manifest failed.")
        for res in data.json().get("resources", []):
            resources.add_resource(res["group"], res["resource_path"], res["resource_size"], res["resource_hash"])
        return resources.to_resource()

    def get_server_url(self, version: str) -> str:
        body = {"market_game_id": "com.nexon.bluearchive", "market_code": "playstore", "curr_build_version": version, "curr_build_number": version.split(".")[-1]}
        resp = FileDownloader(Config.GL_MANIFEST_URL, request_method="post", json=body).get_response()
        return resp.json().get("patch", {}).get("resource_path", "") if resp else ""


class CNServer(BaseServer):
    def __init__(self) -> None:
        self.urls = {
            "home": "https://bluearchive-cn.com/",
            "version": "https://bluearchive-cn.com/api/meta/setup",
            "info": "https://gs-api.bluearchive-cn.com/api/state",
            "bili": "https://line1-h5-pc-api.biligame.com/game/detail/gameinfo?game_base_id=109864",
        }

    def download_apk(self, apk_url: str) -> tuple:
        print(f"[*] 开始获取 APK 信息: {apk_url}")
        downloader_head = FileDownloader(apk_url, request_method="head", use_cloud_scraper=True, verbose=True)
        apk_head = downloader_head.get_response()

        last_modified_beijing = ""
        if apk_head and (last_modified_raw := apk_head.headers.get("last-modified")):
            try:
                dt_utc = datetime.strptime(last_modified_raw, "%a, %d %b %Y %H:%M:%S %Z")
                dt_beijing = dt_utc.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                last_modified_beijing = dt_beijing.strftime("%Y-%m-%d %H:%M:%S")
                print(f"[+] 远程文件最后修改时间 (北京): {last_modified_beijing}")
            except Exception as e:
                print(f"[!] 时间解析失败: {e}")

        apk_size = int(apk_head.headers.get("content-length", 0)) if apk_head else 0
        print(f"[+] 远程文件大小: {apk_size / 1024 / 1024:.2f} MB")

        apk_path = os.path.join(Config.TEMP_DIR, "BlueArchive.apk")
        if os.path.exists(apk_path):
            local_size = os.path.getsize(apk_path)
            if local_size == apk_size:
                print(f"[#] 检测到完整本地缓存，跳过下载: {apk_path}")
                return apk_path, last_modified_beijing
            else:
                print(f"[!] 本地文件大小 ({local_size}) 与远程不符，准备重新下载。")

        print(f"[*] 准备下载整个 APK 文件...")

        FileDownloader(apk_url, request_method="get", enable_progress=True, use_cloud_scraper=True).save_file(apk_path)

        print(f"[+] APK 下载完成: {apk_path}")
        return apk_path


    def get_apk_url(self, server: str = "official") -> str:
        print("获取apk链接")
        if server == "bili":
            resp = FileDownloader(self.urls["bili"]).get_response()
            return resp.json()["data"]["android_download_link"]
        
        home = FileDownloader(self.urls["home"]).get_response()
        js_match = re.search(r'src="([^"]+?\.js)', home.text)
        if js_match:
            js_url = js_match.group(1)
            js_txt = FileDownloader(js_url).get_response().text
            apk_match = re.search(r'http[s]?://[^\s"<>]+?\.apk', js_txt)
            if apk_match:
                print(f"成功从官网 JS 获取链接: {apk_match.group()}")
                return apk_match.group()

        print("官网获取失败，回退至 Bilibili 渠道")
        return self.get_apk_url("bili")

    def get_latest_version(self) -> str:
        resp = FileDownloader(self.urls["version"]).get_response()
        if resp and (m := re.search(r"(\d+\.\d+\.\d+)", resp.text)): return m.group(1)
        raise LookupError("Version failed.")

    def get_resource_manifest(self, sinfo: dict) -> Resource:
        res = CNResource()
        base = sinfo["AddressablesCatalogUrlRoots"][0] + "/"
        res.set_url_link(base, "AssetBundles/Android/", "pool/MediaResources/", "pool/TableBundles/")
        
        map_info = [
            (f"Manifest/TableBundles/{sinfo['TableVersion']}/TableManifest", "table"),
            (f"Manifest/MediaResources/{sinfo['MediaVersion']}/MediaManifest", "media"),
            (f"AssetBundles/Catalog/{sinfo['ResourceVersion']}/Android/bundleDownloadInfo.json", "bundle")
        ]
        
        print("::group::解析资源清单")
        for url_suffix, mode in map_info:
            target_url = urljoin(base, url_suffix)
            print(f"正在获取 {mode} 清单: {target_url}")
            if data := FileDownloader(target_url).get_bytes():
                CNCatalogDecoder.decode_to_manifest(data, res, mode)
        print("::endgroup::")
        return res.to_resource()

    def get_server_info(self, version: str) -> dict:
        resp = FileDownloader(self.urls["info"], headers={"APP-VER": version, "PLATFORM-ID": "1", "CHANNEL-ID": "2"}).get_response()
        if not resp: raise LookupError("Server info failed.")
        return resp.json()

class CNCatalogDecoder:
    media_types = {1: "ogg", 2: "mp4", 3: "jpg", 4: "png", 5: "acb", 6: "awb"}

    @staticmethod
    def decode_to_manifest(
        raw_data: bytes,
        container: CNResource,
        type: Literal["bundle", "table", "media"],
    ) -> dict[str, object]:
        """Used to decode bytes file to readable data structure. Data will return decoded and add to container.

        Args:
            raw_data (bytes): Binary data.
            container (CNResource): Container to storage manifest.
            type (Literal[&quot;table&quot;, &quot;media&quot;]): Data type.

        Returns:
            dict([str, object]): Manifest list.
        """
        manifest: dict[str, object] = {}
        data = BytesIO(raw_data)

        if type == "bundle":
            bundle_data = json.loads(raw_data)
            for item in bundle_data["BundleFiles"]:
                CNCatalogDecoder.__decode_bundle_manifest(item, container)

        if type == "media":
            while item := data.readline():
                key, obj = CNCatalogDecoder.__decode_media_manifest(item, container)
                manifest[key] = obj

        if type == "table":
            table_data: dict = json.loads(raw_data).get("Table", {})
            for file, item in table_data.items():
                key, obj = CNCatalogDecoder.__decode_table_manifest(
                    file, item, container
                )
                manifest[key] = obj

        return manifest

    @classmethod
    def __decode_bundle_manifest(
        cls, data: dict, container: CNResource
    ) -> tuple[str, dict[str, object]]:
        container.add_bundle_resource(
            data["Name"],
            data["Size"],
            data["Crc"],
            data["IsPrologue"],
            data["IsSplitDownload"],
        )

        return data["Name"], {
            "name": data["Name"],
            "size": data["Size"],
            "md5": data["Crc"],
            "is_prologue": data["IsPrologue"],
            "is_split_download": data["IsSplitDownload"],
        }

    @classmethod
    def __decode_media_manifest(
        cls, data: bytes, container: CNResource
    ) -> tuple[str, dict[str, object]]:
        path, md5, media_type_str, size_str, _ = data.decode().split(",")

        media_type = int(media_type_str)
        size = int(size_str)

        if media_type in cls.media_types:
            path += f".{cls.media_types[media_type]}"
        else:
            notice(f"Unidentifiable media type {media_type}.")
        file_url_path = md5[:2] + "/" + md5

        container.add_media_resource(
            file_url_path, path, cls.media_types[media_type], size, md5
        )

        file_path, file_name = os.path.split(path)
        return file_url_path, {
            "path": file_path,
            "file_name": file_name,
            "type": media_type,
            "bytes": size,
            "md5": md5,
        }

    @classmethod
    def __decode_table_manifest(
        cls, key: str, item: dict, container: CNResource
    ) -> tuple[str, dict[str, object]]:

        path: str = item["Name"]
        md5: str = item["Crc"]
        size: int = item["Size"]
        includes: list = item["Includes"]

        size = int(size)
        file_url_path = md5[:2] + "/" + md5

        container.add_table_resource(file_url_path, path, size, md5, includes)

        return key, {
            "name": item["Name"],
            "size": item["Size"],
            "crc": item["Crc"],
            "includes": item["Includes"],
        }

I8: str = "b"
I32: str = "i"
I64: str = "q"
BOOL: str = "?"


class JPCatalogDecoder:

    class Reader:
        def __init__(self, initial_bytes) -> None:
            self.io = BytesIO(initial_bytes)

        def read(self, fmt: str) -> Any:
            """Use struct read bytes by giving a format char."""
            return struct.unpack(fmt, self.io.read(struct.calcsize(fmt)))[0]

        def read_string(self) -> str:
            """Read string."""
            return self.io.read(self.read(I32)).decode(
                encoding="utf-8", errors="replace"
            )

        def read_table_includes(self) -> list[str]:
            """Read talbe inculdes."""
            size = self.read(I32)
            if size == -1:
                return []
            self.read(I32)
            includes = []
            for i in range(size):
                includes.append(self.read_string())
                if i != size - 1:
                    self.read(I32)
            return includes

    @staticmethod
    def decode_to_manifest(
        raw_data: bytes, container: JPResource, type: Literal["table", "media"]
    ) -> dict[str, object]:
        """Used to decode bytes file to readable data structure. Data will return decoded and add to container.

        Args:
            raw_data (bytes): Binary data.
            container (JPResource): Container to storage manifest.
            type (Literal[&quot;table&quot;, &quot;media&quot;]): Data type.

        Returns:
            dict([str, object]): Manifest list.
        """
        data = JPCatalogDecoder.Reader(raw_data)

        manifest: dict[str, object] = {}

        data.read(I8)
        item_num = data.read(I32)

        for _ in range(item_num):
            if type == "media":
                key, obj = JPCatalogDecoder.__decode_media_manifest(data, container)
            else:
                key, obj = JPCatalogDecoder.__decode_table_manifest(data, container)
            manifest[key] = obj
        return manifest

    @classmethod
    def __decode_media_manifest(
        cls, data: Reader, container: JPResource
    ) -> tuple[str, dict[str, object]]:
        data.read(I32)
        key = data.read_string()
        data.read(I8)
        data.read(I32)
        path = data.read_string()
        data.read(I32)
        file_name = data.read_string()
        bytes = data.read(I64)
        crc = data.read(I64)
        is_prologue = data.read(BOOL)
        is_split_download = data.read(BOOL)
        media_type = data.read(I32)

        path = path.replace("\\", "/")
        container.add_media_resource(
            key, path, file_name, media_type, bytes, crc, is_prologue, is_split_download
        )

        return key, {
            "path": path,
            "file_name": file_name,
            "type": media_type,
            "bytes": bytes,
            "crc": crc,
            "is_prologue": is_prologue,
            "is_split_download": is_split_download,
        }

    @classmethod
    def __decode_table_manifest(
        cls, data: Reader, container: JPResource
    ) -> tuple[str, dict[str, object]]:
        data.read(I32)
        key = data.read_string()
        data.read(I8)
        data.read(I32)
        name = data.read_string()
        size = data.read(I64)
        crc = data.read(I64)
        is_in_build = data.read(BOOL)
        is_changed = data.read(BOOL)
        is_prologue = data.read(BOOL)
        is_split_download = data.read(BOOL)
        includes = data.read_table_includes()

        container.add_table_resource(
            key,
            name,
            size,
            crc,
            is_in_build,
            is_changed,
            is_prologue,
            is_split_download,
            includes,
        )

        return key, {
            "name": name,
            "size": size,
            "crc": crc,
            "is_in_build": is_in_build,
            "is_changed": is_changed,
            "is_prologue": is_prologue,
            "is_split_download": is_split_download,
            "includes": includes,
        }

class ServerInfoUrlRewriter:
    def __init__(self):
        self.original_config = None
        self.original_url = None
        self.modified_url = None
        self.modified_config = None
        self.crc_tool = CRCManip()
        self.extractor = BundleExtractor()

    def get_gamemainconfig(self, path):
        results = self.extractor.search_unity_pack(
            path, 
            data_type=["TextAsset"], 
            data_name=["GameMainConfig"], 
            condition_connect=True
        )
        if results:
            obj = results[0]
            data = obj.read()
            original_config = data.m_Script.encode("utf-8", "surrogateescape")
            self.original_config = base64.b64encode(original_config).decode("utf-8", "surrogateescape")
            return original_config

    def get_serverinfo_url(self, original_config=None):
        if original_config is None: original_config = self.original_config
        decrypted_config = convert_string(original_config, create_key("GameMainConfig"))
        config_json = json.loads(decrypted_config)
        encrypted_url = config_json["X04YXBFqd3ZpTg9cKmpvdmpOElwnamB2eE4cXDZqc3ZgTg=="]
        decrypted_url = convert_string(encrypted_url, create_key("ServerInfoDataUrl"))
        self.original_url = decrypted_url
        return decrypted_url

    def modify_serverinfo_url(self, target_url, original_url=None):
        if original_url is None: original_url = self.original_url
        filename = os.path.basename(urlparse(original_url).path)
        if not urlparse(target_url).scheme:
            target_url = "https://" + target_url
        if not target_url.endswith("/"):
            target_url += "/"
        modified_url = target_url + filename
        self.modified_url = modified_url
        return modified_url

    def modify_config(self, original_config=None, modified_url=None):
        if original_config is None: original_config = self.original_config
        if modified_url is None: modified_url = self.modified_url
        decrypted_config = convert_string(original_config, create_key("GameMainConfig"))
        encrypted_url = encrypt_string(modified_url, create_key("ServerInfoDataUrl"))
        config_json = json.loads(decrypted_config)
        config_json["X04YXBFqd3ZpTg9cKmpvdmpOElwnamB2eE4cXDZqc3ZgTg=="] = encrypted_url
        encrypted_config = encrypt_string(json.dumps(config_json, separators=(',', ':')), create_key("GameMainConfig"))
        self.modified_config = encrypted_config
        return encrypted_config

    def fast_rewrite(self, path, target_url):
        self.get_gamemainconfig(path)
        self.get_serverinfo_url()
        self.modify_serverinfo_url(target_url)
        self.modify_config()
        
        new_content = base64.b64decode(self.modified_config).decode("utf-8", "surrogateescape")
        self.extractor.modify_and_replace(path, "GameMainConfig", new_content)

class SdkUrlRewriter:
    def __init__(self):
        self.original_config = None
        self.modified_config = None
        self.original_url = None
        self.modified_url = None
        self.json_file_path = None
        self.original_crc = None
        self.original_size = None
        self.crc_tool = CRCManip()
        self.extractor = BundleExtractor()

    def get_sdkconfigsettings(self, path, is_unity_file=True):
        if is_unity_file:
            results = self.extractor.search_unity_pack(path, data_type=["TextAsset"], data_name=["SDKConfigSettings"], condition_connect=True)
            if results:
                self.original_config = results[0].read().m_Script
                return self.original_config
        else:
            for root, dirs, files in os.walk(path):
                for filename in files:
                    if filename.endswith('.json'):
                        filepath = os.path.join(root, filename)
                        with open(filepath, 'r', encoding='utf-8') as f:
                            content = f.read()
                            try:
                                config_json = json.loads(content)
                                if "Regions" in config_json and "Jp" in config_json["Regions"]:
                                    self.original_config = content
                                    self.json_file_path = filepath
                                    self.original_size = os.path.getsize(filepath)
                                    self.original_crc = self.crc_tool.get_crc(filepath).strip()
                                    return content
                            except: continue
        return None
        
    def get_sdk_url(self, original_config=None):
        if original_config is None: original_config = self.original_config
        config_json = json.loads(original_config)
        self.original_url = config_json["Regions"]["Jp"]["Sdk_Url"]
        return self.original_url

    def modify_sdk_url(self, target_url):
        if not urlparse(target_url).scheme: target_url = "https://" + target_url
        self.modified_url = target_url
        return target_url

    def modify_config(self, original_config=None, modified_url=None, plist_changes=None):
        if original_config is None: original_config = self.original_config
        if modified_url is None: modified_url = self.modified_url
        config_json = json.loads(original_config)
        config_json["Regions"]["Jp"]["Sdk_Url"] = modified_url
        if plist_changes and "Plist" in config_json and "Jp" in config_json["Plist"]:
            for key, value in plist_changes.items(): config_json["Plist"]["Jp"][key] = value
        config_json["__crc__"] = ""
        self.modified_config = json.dumps(config_json, separators=(',', ':'), ensure_ascii=False)
        return self.modified_config

    def fast_rewrite(self, path, target_url, plist_changes=None, is_unity_file=True):
        if not self.get_sdkconfigsettings(path, is_unity_file): return
        self.get_sdk_url()
        self.modify_sdk_url(target_url)
        self.modify_config(plist_changes=plist_changes)
        
        if is_unity_file:
            self.extractor.modify_and_replace(path, "SDKConfigSettings", self.modified_config)
        else:
            if self.json_file_path:
                with open(self.json_file_path, 'w', encoding='utf-8') as f:
                    f.write(self.modified_config)
                temp_output = self.json_file_path + ".tmp"
                self.crc_tool.insert(self.json_file_path, temp_output, self.original_crc, position=-2, target_size_bytes=self.original_size, adjust_position=-2)
                if os.path.exists(temp_output):
                    os.replace(temp_output, self.json_file_path)
        print("SDKConfigSettings已完成写回")
