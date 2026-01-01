import os
import zipfile
import subprocess
import shutil
import re
from pathlib import Path
from lxml import etree
from utils.util import ZipUtils
from utils.config import Config
from os import path

class APKTools:
    def __init__(self):
        self.target_apks = {
            "main": "com.YostarJP.BlueArchive.apk",
            "config": "config.arm64_v8a.apk", 
            "unity": "UnityDataAssetPack.apk"
        }
        self.APK_EXTRACT_FOLDER = path.join(Config.TEMP_DIR, "Extract")
        self.MAIN_APK_PATH = path.join(Config.TEMP_DIR, "original_main.apk")
        self.DECODED_FOLDER = path.join(Config.TEMP_DIR, "decoded_apk")
        self.REPACKED_APK = path.join(Config.TEMP_DIR, "repacked.apk")
        current_dir = Path(__file__).parent.parent
        self.APKTOOL_JAR = current_dir / "apktool.jar"
        self.APKSIGN_JAR = current_dir / "apksigner.jar"

    def extract_apk_file(self, apk_path):
        temp_dir = path.join(Config.TEMP_DIR)
        apk_files = ZipUtils.extract_zip(apk_path, temp_dir, keywords=["apk"])
        os.makedirs(self.APK_EXTRACT_FOLDER, exist_ok=True)
        apk_paths = {}
        for apk_file in apk_files:
            full_path = apk_file if path.isabs(apk_file) else path.join(temp_dir, apk_file)
            if self.target_apks["main"] in apk_file:
                apk_paths["main"] = full_path
                shutil.copy2(full_path, self.MAIN_APK_PATH)
            elif self.target_apks["config"] in apk_file:
                apk_paths["config"] = full_path
            elif self.target_apks["unity"] in apk_file:
                apk_paths["unity"] = full_path
        self._decode_and_merge(apk_paths)
        self._modify()

    def _run_apktool(self, args):
        java_cmd = ['java', '-jar', str(self.APKTOOL_JAR)] + args
        result = subprocess.run(java_cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise Exception(f"apktool failed: {result.stderr}")
        return result

    def _decode_and_merge(self, apk_paths):
        if "main" in apk_paths:
            args = ['d', apk_paths['main'], '-o', self.DECODED_FOLDER, '-f', '--no-src']
            self._run_apktool(args)
        
        for apk_type, apk_path in apk_paths.items():
            if apk_type == "main": continue
            temp_decode_dir = path.join(Config.TEMP_DIR, f"temp_decode_{apk_type}")
            args = ['d', apk_path, '-o', temp_decode_dir, '-f']
            self._run_apktool(args)
            if apk_type == "config":
                self._merge_specific_folder(temp_decode_dir, self.DECODED_FOLDER, "lib")
            elif apk_type == "unity":
                self._merge_specific_folder(temp_decode_dir, self.DECODED_FOLDER, "assets")
            shutil.rmtree(temp_decode_dir)

    def _merge_specific_folder(self, src_dir, dst_dir, folder_name):
        src_folder = os.path.join(src_dir, folder_name)
        dst_folder = os.path.join(dst_dir, folder_name)
        if os.path.exists(src_folder):
            for root, dirs, files in os.walk(src_folder):
                rel_path = os.path.relpath(root, src_folder)
                target_dir = os.path.join(dst_folder, rel_path)
                os.makedirs(target_dir, exist_ok=True)
                for file in files:
                    src_file = os.path.join(root, file)
                    dst_file = os.path.join(target_dir, file)
                    if not os.path.exists(dst_file):
                        shutil.copy2(src_file, dst_file)

    def _modify(self):
        self._modify_manifest()
        self._modify_resources()

    def _modify_manifest(self):
        manifest_path = Path(self.DECODED_FOLDER) / "AndroidManifest.xml"       
        with open(manifest_path, 'r', encoding='utf-8') as f:
            content = f.read()

        host_pattern = r'(android:host=")([^"]+)(")'
        host_matches = list(re.finditer(host_pattern, content))
        host_values = [match.group(2) for match in host_matches]
        temp_content = content

        for i, match in enumerate(host_matches):
            temp_marker = f"__HOST_TEMP_{i}__"
            temp_content = temp_content.replace(match.group(0), f'{match.group(1)}{temp_marker}{match.group(3)}')
            temp_content = temp_content.replace('com.YostarJP.BlueArchive', 'com.BCJP.BlueArchive')

        patterns_to_prefix = [
            'com.google.android.gms.permission.AD_ID',
            'com.facebook.katana.provider.PlatformProvider',
            'com.google.android.finsky.permission.BIND_GET_INSTALL_REFERRER_SERVICE',
            'com.google.android.c2dm.permission.RECEIVE',
            'android.permission.CHANGE_NETWORK_STATE',
            'android.permission.WRITE_SETTINGS'
        ]    

        for pattern in patterns_to_prefix:
            temp_content = temp_content.replace(pattern, f'com.BCJP.BlueArchive_{pattern}')

        for i, original_host in enumerate(host_values):
            temp_marker = f"__HOST_TEMP_{i}__"
            temp_content = temp_content.replace(temp_marker, original_host)

        tree = etree.fromstring(temp_content.encode('utf-8'))
        root = tree
        for attr in ['{http://schemas.android.com/apk/res/android}requiredSplitTypes', '{http://schemas.android.com/apk/res/android}splitTypes']:
            if attr in root.attrib: 
                del root.attrib[attr]
        
        ns = {'android': 'http://schemas.android.com/apk/res/android'}
        for meta in root.findall(".//meta-data", namespaces=ns):
            name = meta.get('{http://schemas.android.com/apk/res/android}name')
            if name == "com.android.vending.splits.required":
                meta.set('{http://schemas.android.com/apk/res/android}name', 'com.android.dynamic.apk.fused.modules')
                meta.set('{http://schemas.android.com/apk/res/android}value', 'UnityDataAssetPack,base')
        
        new_xml = etree.tostring(root, encoding='utf-8', pretty_print=True).decode('utf-8')
        with open(manifest_path, 'w', encoding='utf-8') as f:
            f.write(new_xml)

    def _modify_resources(self):
        strings_files = list(Path(self.DECODED_FOLDER).glob("res/values*/strings.xml"))
        for strings_path in strings_files:
            with open(strings_path, 'r', encoding='utf-8') as f:
                content = f.read()
            if '<string name="app_name">ブルアカ</string>' in content:
                content = content.replace('<string name="app_name">ブルアカ</string>', '<string name="app_name">蔚蓝档案</string>')
                with open(strings_path, 'w', encoding='utf-8') as f:
                    f.write(content)
        
        package_info_files = list(Path(self.DECODED_FOLDER).glob("res/values*/package-info.xml"))
        for package_info_path in package_info_files:
            with open(package_info_path, 'r', encoding='utf-8') as f:
                content = f.read()
            content = content.replace('name="com.YostarJP.BlueArchive"', 'name="com.BCJP.BlueArchive"')
            with open(package_info_path, 'w', encoding='utf-8') as f:
                f.write(content)

    def build(self, decoded_folder, repacked_apk):
        args = ['b', decoded_folder, '-o', repacked_apk, '-f']
        self._run_apktool(args)
        self._restore_original_dex(repacked_apk)
        self._fix_apk_alignment(repacked_apk)

    def _restore_original_dex(self, target_apk):
        dex_map = {}
        with zipfile.ZipFile(self.MAIN_APK_PATH, 'r') as z_orig:
            for name in z_orig.namelist():
                if name.endswith('.dex'):
                    dex_map[name] = z_orig.read(name)
        
        temp_apk = target_apk + ".dex_fix"
        with zipfile.ZipFile(target_apk, 'r') as z_in, \
             zipfile.ZipFile(temp_apk, 'w') as z_out:
            for item in z_in.infolist():
                if item.filename in dex_map:
                    z_out.writestr(item.filename, dex_map[item.filename], compress_type=zipfile.ZIP_DEFLATED)
                    del dex_map[item.filename]
                else:
                    z_out.writestr(item, z_in.read(item.filename))
            for name, data in dex_map.items():
                z_out.writestr(name, data, compress_type=zipfile.ZIP_DEFLATED)
        shutil.move(temp_apk, target_apk)

    def _fix_apk_alignment(self, apk_path):
        temp_apk = apk_path + ".align_tmp"
        with zipfile.ZipFile(apk_path, 'r') as z_in, \
             zipfile.ZipFile(temp_apk, 'w') as z_out:
            
            arsc_data = None
            arsc_info = None

            for item in z_in.infolist():
                if item.filename == 'resources.arsc':
                    arsc_data = z_in.read(item.filename)
                    arsc_info = item
                    continue
                
                data = z_in.read(item.filename)
                new_info = zipfile.ZipInfo(item.filename)
                new_info.date_time = (1981, 1, 1, 0, 0, 0)
                
                if item.filename == 'AndroidManifest.xml' or item.filename.startswith('assets/'):
                    new_info.compress_type = zipfile.ZIP_STORED
                else:
                    new_info.compress_type = zipfile.ZIP_DEFLATED
                
                z_out.writestr(new_info, data)

            if arsc_data and arsc_info:
                new_arsc_info = zipfile.ZipInfo('resources.arsc')
                new_arsc_info.date_time = (1981, 1, 1, 0, 0, 0)
                new_arsc_info.compress_type = zipfile.ZIP_STORED
                
                current_offset = z_out.fp.tell()
                header_size = 30 + len(new_arsc_info.filename)
                data_offset = current_offset + header_size
                padding = (4 - (data_offset % 4)) % 4
                
                if padding > 0:
                    new_arsc_info.extra = b'\x00' * padding
                
                z_out.writestr(new_arsc_info, arsc_data)

        shutil.move(temp_apk, apk_path)

    def sign(self, apk_path, out_path):
        keystore_path = Path("beichen.jks")
        cmd = [
            'java', '-jar', str(self.APKSIGN_JAR), 'sign',
            '--ks', str(keystore_path),
            '--ks-pass', 'pass:北辰汉化组a',
            '--key-pass', 'pass:北辰汉化组a',
            '--out', out_path,
            '--v1-signing-enabled', 'true',
            '--v2-signing-enabled', 'true',
            '--v3-signing-enabled', 'true',
            apk_path
        ]
        subprocess.run(cmd, capture_output=True, text=True)

    def clean(self):
        for item in Path(Config.TEMP_DIR).iterdir():
            if item.name == "蔚蓝档案.apk": continue        
            if item.is_file(): item.unlink()
            elif item.is_dir(): shutil.rmtree(item)
