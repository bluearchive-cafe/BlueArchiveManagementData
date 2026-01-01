import json
import multiprocessing
import multiprocessing.queues
import multiprocessing.synchronize
import os
from os import path
from typing import Any, Literal, List, Optional, Union

import UnityPy
from PIL import Image
from lib.console import ProgressBar
from utils.util import CRCManip

class BundleExtractor:
    MAIN_EXTRACT_TYPES = [
        "Texture2D",
        "Sprite",
        "AudioClip",
        "Font",
        "TextAsset",
        "Mesh",
        "VideoClip",
        "MonoBehaviour",
        "Shader"
    ]
    
    def __init__(self, EXTRACT_DIR: str = "output", BUNDLE_FOLDER: str = "") -> None:
        self.BUNDLE_FOLDER = BUNDLE_FOLDER
        self.BUNDLE_EXTRACT_FOLDER = EXTRACT_DIR

    def __save(
        self, type: Literal["json", "binary", "mesh"], save_path: str, data: Any
    ) -> None:
        if data is None: return
        if type == "json":
            with open(save_path, "wt", encoding="utf8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        elif type == "binary":
            with open(save_path, "wb") as f:
                f.write(data)
        elif type == "mesh":
            with open(save_path, "wt", encoding="utf8", newline="") as f:
                f.write(data)

    def __get_raw_data(self, data: Any, res_path: str) -> Optional[bytes]:
        raw = None
        for attr in ["m_VideoData", "m_FontData", "m_AudioData", "m_Script", "m_Stream"]:
            if hasattr(data, attr):
                val = getattr(data, attr)
                if val:
                    raw = val
                    break
        
        if not raw and hasattr(data, "m_ExternalResources"):
            ext = data.m_ExternalResources
            res_file = path.join(path.dirname(res_path), path.basename(ext.m_Source))
            if path.exists(res_file):
                with open(res_file, "rb") as f:
                    f.seek(ext.m_Offset)
                    raw = f.read(ext.m_Size)
        
        if isinstance(raw, list):
            raw = bytes(raw)
        elif isinstance(raw, str):
            raw = raw.encode("utf-8", "surrogateescape")
            
        return raw

    def search_unity_pack(
        self,
        pack_path: str,
        data_type: Optional[List[str]] = None,
        data_name: Optional[List[str]] = None,
        condition_connect: bool = False,
        read_obj_anyway: bool = False,
    ) -> List[Any]:
        data_list = []
        if os.path.isdir(pack_path):
            for root, _, files in os.walk(pack_path):
                for filename in files:
                    data_list.extend(self.search_unity_pack(path.join(root, filename), data_type, data_name, condition_connect, read_obj_anyway))
            return data_list

        try:
            env = UnityPy.load(pack_path)
            for obj in env.objects:
                type_passed = data_type and obj.type.name in data_type
                if type_passed and not condition_connect:
                    obj.source_path = pack_path
                    data_list.append(obj)
                
                if read_obj_anyway or (type_passed and condition_connect) or not data_type:
                    data = obj.read()
                    if data_name and hasattr(data, "m_Name") and data.m_Name in data_name:
                        obj.source_path = pack_path
                        data_list.append(obj)
        except: pass
        return data_list

    def modify_and_replace(self, folder_path: str, asset_name: str, new_data: Any):
        crc_tool = CRCManip()
        targets = []
        if os.path.isdir(folder_path):
            for root, _, files in os.walk(folder_path):
                for filename in files: targets.append(path.join(root, filename))
        else: targets.append(folder_path)

        for filepath in targets:
            try:
                original_size, original_crc = os.path.getsize(filepath), crc_tool.get_crc(filepath).strip()
                env = UnityPy.load(filepath)
                modified = False
                for obj in env.objects:
                    data = obj.read()
                    if not (hasattr(data, "m_Name") and data.m_Name == asset_name):
                        continue

                    obj_type = obj.type.name
                    if obj_type == "TextAsset":
                        data.m_Script = new_data.decode('utf-8') if isinstance(new_data, bytes) else str(new_data)
                        data.save(); modified = True
                    elif obj_type == "Texture2D" and isinstance(new_data, Image.Image):
                        data.image = new_data; data.save(); modified = True
                    elif obj_type == "Texture2D" and isinstance(new_data, (bytes, list)):
                        data.m_CompleteImage = bytes(new_data) if isinstance(new_data, list) else new_data
                        data.save(); modified = True
                    elif obj_type == "AudioClip" and isinstance(new_data, dict):
                        data.samples = new_data; data.save(); modified = True
                    elif obj_type == "VideoClip" and isinstance(new_data, (bytes, list)):
                        data.m_VideoData = bytes(new_data) if isinstance(new_data, list) else new_data
                        data.save(); modified = True
                    elif obj_type == "Font" and isinstance(new_data, (bytes, list)):
                        data.m_FontData = bytes(new_data) if isinstance(new_data, list) else new_data
                        data.save(); modified = True
                    elif obj_type == "MonoBehaviour" and isinstance(new_data, dict):
                        obj.save_typetree(new_data); modified = True
                
                if modified:
                    with open(filepath, "wb") as f: f.write(env.file.save())
                    temp_output = filepath + ".tmp"
                    crc_tool.normal_execute(filepath, temp_output, original_crc, original_size)
                    if os.path.exists(temp_output): os.replace(temp_output, filepath)
            except: continue

    def extract_bundle(self, res_path: str, extract_types: Optional[List[str]] = None) -> None:
        counter: dict[str, int] = {}
        env = UnityPy.load(res_path)
        types_to_extract = extract_types or self.MAIN_EXTRACT_TYPES
        
        for obj in env.objects:
            obj_type = obj.type.name
            if obj_type not in types_to_extract: continue

            try:
                data = obj.read()
                extract_folder = path.join(self.BUNDLE_EXTRACT_FOLDER, obj_type)
                os.makedirs(extract_folder, exist_ok=True)
                
                if obj_type in ["VideoClip", "Font", "AudioClip", "TextAsset"]:
                    raw_data = self.__get_raw_data(data, res_path)
                    if raw_data:
                        ext = ""
                        if obj_type == "VideoClip":
                            ext = ".webm" if raw_data.startswith(b'\x1a\x45\xdf\xa3') else ".mp4"
                        elif obj_type == "Font":
                            ext = ".otf" if raw_data.startswith(b"OTTO") else ".ttf"
                        elif obj_type == "AudioClip":
                            for name, sample_data in data.samples.items():
                                self.__save("binary", path.join(extract_folder, name), sample_data)
                            continue
                        elif obj_type == "TextAsset":
                            ext = ".txt"
                            
                        file_name = f"{data.m_Name}{ext}" if data.m_Name else f"{obj.path_id}{ext}"
                        self.__save("binary", path.join(extract_folder, file_name), raw_data)
                    continue

                if obj_type in ["Texture2D", "Sprite"]:
                    data.image.save(path.join(extract_folder, f"{data.m_Name or obj.path_id}.png"))
                    continue

                if obj_type == "Mesh":
                    try:
                        mesh_str = data.export()
                        self.__save("mesh", path.join(extract_folder, f"{data.m_Name or obj.path_id}.obj"), mesh_str)
                    except: pass
                    continue

                type_tree = obj.read_typetree()
                name = type_tree.get("m_Name") or f"{obj_type}_{obj.path_id}"
                self.__save("json", path.join(extract_folder, f"{name}.json"), type_tree)

            except: continue

    def multiprocess_extract_worker(self, tasks: multiprocessing.Queue, extract_types: Optional[List[str]]) -> None:
        while not tasks.empty():
            try:
                bundle_path = tasks.get_nowait()
                ProgressBar.item_text(path.basename(bundle_path))
                self.extract_bundle(bundle_path, extract_types)
            except: pass
