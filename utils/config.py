from typing import Optional

class Config:
    version: Optional[str] = None
    
    JP_INFO_URL: str = "https://prod-noticeindex.bluearchiveyostar.com/prod/index.json"
    JP_UPTODOWN_URL: str = "https://blue-archive.jp.uptodown.com/android"
    JP_APKPURE_URL: str = "https://d.apkpure.com/b/XAPK/com.YostarJP.BlueArchive"

    GL_APKPURE_URL: str = "https://d.apkpure.com/b/XAPK/com.nexon.bluearchive"
    GL_MANIFEST_URL = "https://api-pub.nexon.com/patch/v1.1/version-check" 
    GL_UPTODOWN_URL = "https://blue-archive-global.en.uptodown.com/android"

    EXTRACT_DIR: str = "Extracted"
    DUMP_PATH: str = "Dumps"
    TEMP_DIR: str = "Temp"

    threads = 32
    max_threads = threads * 7
    is_cn = False # 国服不进行Excel.zip的xor解密
    is_jp = False # 日服对DB解密
    is_gl = False # 国际服未对il2cpp加密
    proxy = None
    retries = 5

    @classmethod
    def set_version(cls, version: str) -> None:
        cls.version = version
        
    @classmethod
    def set_temp_dir(cls, temp_dir: str) -> None:
        cls.temp_dir = temp_dir
