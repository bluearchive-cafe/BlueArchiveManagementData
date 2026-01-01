import argparse
import os
from os import path
from lib.dumper import IL2CppDumper
from utils.config import Config

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('region', choices=['CN', 'JP', 'GL'], nargs='?', default='JP')
    args = parser.parse_args()
    
    Config.is_cn = (args.region == 'CN')
    Config.is_jp = (args.region == 'JP')
    Config.is_gl = (args.region == 'GL')
    
    region = args.region
    
    if region == 'JP':
        from update.regions import JPServer
        server = JPServer()
        apk_path = server.download_xapk("https://d.apkpure.net/b/XAPK/com.YostarJP.BlueArchive?version=latest&nc=arm64-v8a&sv=24")
    elif region == 'GL':
        from update.regions import GLServer
        server = GLServer()
        apk_path = server.download_xapk("https://d.apkpure.net/b/XAPK/com.nexon.bluearchive?version=latest&nc=arm64-v8a&sv=24")

    if region != 'CN':
        from update.regions import BaseServer
        BaseServer().extract_apks_file(apk_path)

    dumper = IL2CppDumper()
    dumper.extract_il2cpp_data(Config.EXTRACT_DIR, Config.TEMP_DIR, Config.DUMP_PATH)

if __name__ == "__main__":
    main()
