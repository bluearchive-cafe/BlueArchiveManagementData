import json
import tempfile
import zipfile
from argparse import ArgumentParser
from pathlib import Path
import sqlite3
import os
from xtractor.table import TableExtractor
from utils.config import Config
from pysqlcipher3 import dbapi2 as sqlite
import binascii

def parse_args():
    p = ArgumentParser(description="Unpack to JSON files.")
    p.add_argument("db_path", type=Path)
    p.add_argument("zip_path", type=Path)
    p.add_argument("flatbuffers_dir", type=Path)
    p.add_argument("output_folder", type=Path)
    p.add_argument("server", type=str, choices=["CN", "GL", "JP"])
    p.add_argument("--db_key", type=str, default="402c4ad5d15be789c8621dd6920fb4d990de9d584a792c0d5a903688fe653c3d", help="Optional database key to set")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    args.output_folder.mkdir(parents=True, exist_ok=True)

    Config.is_cn = (args.server == "CN")
    Config.is_jp = (args.server == "JP")

    flat_data_module_name = ".".join(args.flatbuffers_dir.parts).lstrip(".")

    db_schema_dir = args.output_folder / "DBSchema"
    db_schema_dir.mkdir(parents=True, exist_ok=True)
    db_extractor = TableExtractor(str(args.db_path.parent), str(db_schema_dir), flat_data_module_name)
    db_extractor.extract_table(args.db_path.name, args.db_key)

    excel_table_dir = args.output_folder / "ExcelTable"
    excel_table_dir.mkdir(parents=True, exist_ok=True)
    zip_extractor = TableExtractor(str(args.zip_path.parent), str(excel_table_dir), flat_data_module_name)
    zip_extractor.extract_table(args.zip_path.name, args.db_key)

