import os
import platform
import json
from os import path
from lib.console import notice, print
from lib.downloader import FileDownloader
from utils.config import Config
from utils.util import CommandUtils, FileUtils, ZipUtils
from lib.compiler import CompileToPython, CSParser, CNCSParser

def get_platform_identifier():
    os_map = {"linux": "linux", "darwin": "osx", "windows": "win"}
    arch_map = {"x86_64": "x64", "amd64": "x64", "arm64": "arm64", "aarch64": "arm64"}

    os_name = os_map.get(platform.system().lower())
    arch = arch_map.get(platform.machine().lower())

    if not os_name or not arch:
        raise RuntimeError(f"Unsupported platform: {platform.system()} {platform.machine()}")

    return f"{os_name}-{arch}", os_name

class IL2CppDumper:
    def __init__(self):
        self.project_dir = ""
        self.binary_name = "Il2CppInspector"
        self.IL2CPP_ZIP = "https://github.com/Perfare/Il2CppDumper/archive/refs/heads/master.zip"
        self.IL2CPP_FOLDER = "Il2CppDumper-master"
        self.ZIP_NAME = "il2cpp-dumper.zip"

    def extract_il2cpp_data(self, extract_dir, temp_dir, dump_path):
        if Config.is_cn:
            dump_cs_path = path.join(dump_path, "dump.cs")
            if not path.exists(dump_cs_path):
                raise FileNotFoundError(f"dump.cs not found: {dump_cs_path}")
            self.compile_python(dump_cs_path, extract_dir)
            return
            
        if not path.exists(path.join(extract_dir, "FlatData")):
            self._perform_il2cpp_extraction(extract_dir, temp_dir, dump_path)

    def _perform_il2cpp_extraction(self, extract_dir, temp_dir, dump_path):
        self._perform_extraction(extract_dir, temp_dir, dump_path)

    def _perform_extraction(self, extract_dir, temp_dir, dump_path):
        save_path = temp_dir
        self.get_il2cpp_dumper_jp(save_path)

        il2cpp_path = FileUtils.find_files(temp_dir, ["libil2cpp.so"], True)
        metadata_path = FileUtils.find_files(temp_dir, ["global-metadata.dat"], True)

        if not (il2cpp_path and metadata_path):
            raise FileNotFoundError("Missing il2cpp binary or metadata file")
        
        abs_il2cpp_path = path.abspath(il2cpp_path[0])
        abs_metadata_path = path.abspath(metadata_path[0])
        extract_path = path.abspath(path.join(extract_dir, dump_path))

        print("Dumping il2cpp...")
        if Config.is_gl:
            self.dump_il2cpp_jp(extract_path, abs_il2cpp_path, abs_metadata_path)
        else:
            self.dump_il2cpp_jp(extract_path, abs_il2cpp_path, abs_metadata_path)
        notice("Dump successful")
        self.compile_python(path.join(extract_path, "dump.cs"), extract_dir)
        notice("FlatData generated: " + extract_dir)

    def get_il2cpp_dumper_jp(self, save_path):
        os.makedirs(save_path, exist_ok=True)
        platform_id, os_name = get_platform_identifier()
        il2cpp_zip_url = f"https://github.com/beichen23333/Il2CppInspectorRedux/releases/latest/download/Il2CppInspectorRedux.CLI-{platform_id}.zip"

        zip_path = path.join(save_path, "Il2CppInspectorRedux.CLI.zip")
        FileDownloader(il2cpp_zip_url).save_file(zip_path)

        extract_path = path.join(save_path, "Il2CppInspector")
        ZipUtils.extract_zip(zip_path, extract_path)
        self.project_dir = extract_path

        if os_name == "win":
            self.binary_name += ".exe"
        else:
            CommandUtils.run_command("chmod", "+x", f"./{self.binary_name}", cwd=self.project_dir)

    def dump_il2cpp_jp(self, extract_path, il2cpp_path, global_metadata_path):
        os.makedirs(extract_path, exist_ok=True)
        binary_path = f"./{self.binary_name}"
        cs_out = path.join(extract_path, "dump.cs")

        success, err = CommandUtils.run_command(
            path.abspath(path.join(self.project_dir, binary_path)),
            "--bin", il2cpp_path,
            "--metadata", global_metadata_path,
            "--select-outputs",
            "--cs-out", cs_out,
            "--must-compile",
            cwd=self.project_dir
        )

        if not success:
            raise RuntimeError(f"IL2CPP dump failed: {err}")

    def get_il2cpp_dumper_gl(self, save_path: str) -> None:
        os.makedirs(save_path, exist_ok=True)
        zip_path = path.join(save_path, self.ZIP_NAME)
        FileDownloader(self.IL2CPP_ZIP).save_file(zip_path)
        ZipUtils.extract_zip(zip_path, save_path)

        config_path = FileUtils.find_files(
            path.join(save_path, self.IL2CPP_FOLDER), ["config.json"], True
        )

        if not config_path:
            raise FileNotFoundError(
                "Cannot find config file. Make sure il2cpp-dumper exists."
            )

        with open(config_path[0], "r+", encoding="utf8") as config:
            il2cpp_config: dict = json.load(config)
            il2cpp_config["RequireAnyKey"] = False
            il2cpp_config["GenerateDummyDll"] = False
            config.seek(0)
            config.truncate()
            json.dump(il2cpp_config, config)

        self.project_dir = path.dirname(config_path[0])

    def dump_il2cpp_gl(self,extract_path: str,il2cpp_path: str,global_metadata_path: str,max_retries: int = 1,) -> None:
        os.makedirs(extract_path, exist_ok=True)
        success, err = CommandUtils.run_command(
            "dotnet",
            "run",
            "--framework",
            "net8.0",
            il2cpp_path,
            global_metadata_path,
            extract_path,
            cwd=self.project_dir,
        )
        if not success:
            if max_retries == 0:
                raise RuntimeError(
                    f"Error occurred during dump the il2cpp file. Retry might solve this issue. Info: {err}"
                )
            return self.dump_il2cpp_gl(
                extract_path, il2cpp_path, global_metadata_path, max_retries - 1
            )

        return None

    def compile_python(self, dump_cs_file_path, extract_dir):
        print("Parsing dump.cs...")
        if Config.is_cn:
            parser = CNCSParser(dump_cs_file_path)
        else:
            parser = CSParser(dump_cs_file_path)
        enums = parser.parse_enum()
        structs = parser.parse_struct()
        print("Generating flatbuffer python files...")
        compiler = CompileToPython(enums, structs, path.join(extract_dir, "FlatData"))
        compiler.create_enum_files()
        compiler.create_struct_files()
        compiler.create_module_file()
        compiler.create_dump_dict_file()
        compiler.create_repack_dict_file()
