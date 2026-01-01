import argparse
import shutil
import json
import subprocess
import tempfile
import flatbuffers
from pathlib import Path
from zipfile import ZipFile
from collections import defaultdict
from xtractor.table import TableRepacker, TableExtractor, Sqlcipher
from utils.config import Config
from lib.encryption import zip_password, xor_with_key

def build_db(excel_dir: Path, repl_input_dir: Path, output_dir: Path, replacements_root: Path) -> None:
    excel_input_path = excel_dir / "ExcelDB.db"
    output_filepath = output_dir / "ExcelDB.db"
    db_schema_dir = repl_input_dir / "DBSchema"
    replacements_dir = replacements_root / "DBSchema"
    
    if not db_schema_dir.exists():
        db_extractor = TableExtractor(str(excel_dir), str(db_schema_dir), flat_data_module_name)
        db_extractor.extract_table("ExcelDB.db", args.db_key)
    else:
        Sqlcipher().decrypt_sqlcipher(str(excel_input_path), "./temp_ExcelDB.db", args.db_key)

    for jsonfile in db_schema_dir.glob("*.json"):
        repl_file = replacements_dir / jsonfile.name
        if repl_file.exists():
            temp_json_path = packer.apply_replacements(jsonfile, repl_file, skip_fields=["VoiceId"])
            packer.repackDB(temp_json_path, excel_input_path, output_filepath)
            if temp_json_path.exists():
                temp_json_path.unlink()
    
    packer.rebuild_database(output_filepath)

def build_zip(excel_dir: Path, repl_input_dir: Path, output_dir: Path, replacements_root: Path) -> None:
    zip_input_path = excel_dir / "Excel.zip"
    output_filepath = output_dir / "Excel.zip"
    zip_schema_dir = repl_input_dir / "ExcelTable"
    replacements_dir = replacements_root / "ExcelTable"
    
    if not zip_schema_dir.exists():
        zip_extractor = TableExtractor(str(excel_dir), str(zip_schema_dir), flat_data_module_name)
        zip_extractor.extract_table("Excel.zip", args.db_key)

    with tempfile.TemporaryDirectory() as temp_extract_dir:
        temp_extract_path = Path(temp_extract_dir)
        
        with ZipFile(zip_input_path, "r") as excel_zip:
            excel_zip.setpassword(zip_password("Excel.zip"))
            excel_zip.extractall(path=temp_extract_path)

        for json_schema in zip_schema_dir.glob("*.json"):
            repl_file = replacements_dir / json_schema.name
            if repl_file.exists():
                target_bytes_name = f"{json_schema.stem.lower()}.bytes"
                target_file_path = temp_extract_path / target_bytes_name
                
                temp_json_path = packer.apply_replacements(json_schema, repl_file)
                
                new_content = packer.repackExcel(temp_json_path)
                
                with open(target_file_path, "wb") as f:
                    f.write(new_content)
                
                if temp_json_path.exists():
                    temp_json_path.unlink()
                    if temp_json_path.parent.name == "temp":
                        try:
                            temp_json_path.parent.rmdir()
                        except OSError:
                            pass

        password_str = zip_password("Excel.zip").decode()
        if output_filepath.exists():
            output_filepath.unlink()

        cmd = ["zip", "-r", "-j", "-9", "-P", password_str, str(output_filepath.resolve()), "."]
        subprocess.run(cmd, cwd=temp_extract_path, check=True, capture_output=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("replacements_dir", type=Path)
    parser.add_argument("flatbuffers_dir", type=Path)
    parser.add_argument("excel_input_path", type=Path)
    parser.add_argument("repl_input_dir", type=Path)
    parser.add_argument("output_filepath", type=Path, nargs="?", default=None)
    parser.add_argument("--db_key", type=str, default="402c4ad5d15be789c8621dd6920fb4d990de9d584a792c0d5a903688fe653c3d")
    args = parser.parse_args()

    flat_data_module_name = ".".join(args.flatbuffers_dir.parts).lstrip(".")
    Config.is_jp = True

    out_dir = args.output_filepath if args.output_filepath else args.excel_input_path

    packer = TableRepacker(flat_data_module_name, args.db_key)

    build_db(args.excel_input_path, args.repl_input_dir, out_dir, args.replacements_dir)
    build_zip(args.excel_input_path, args.repl_input_dir, out_dir, args.replacements_dir)
