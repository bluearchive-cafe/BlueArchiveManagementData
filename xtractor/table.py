import importlib
import json
import os
from os import path
from types import ModuleType
from typing import Any
from zipfile import ZipFile
from pysqlcipher3 import dbapi2 as sqlite
import sqlcipher3
import sqlite3
import tempfile
import shutil
from collections import defaultdict
import flatbuffers
from lib.console import notice
from lib.encryption import xor_with_key, zip_password
from lib.structure import DBTable, SQLiteDataType
from utils.database import TableDatabase
from utils.config import Config
import subprocess
from pathlib import Path

class TableExtractor:
    def __init__(
        self, table_file_folder: str, extract_folder: str, flat_data_module_name: str
    ) -> None:
        self.table_file_folder = table_file_folder
        self.extract_folder = extract_folder
        self.flat_data_module_name = flat_data_module_name

        self.lower_fb_name_modules: dict[str, type] = {}
        self.dump_wrapper_lib: ModuleType

        self.__import_modules()

    def __import_modules(self):
        try:
            flat_data_lib = importlib.import_module(self.flat_data_module_name)
            self.dump_wrapper_lib = importlib.import_module(
                f"{self.flat_data_module_name}.dump_wrapper"
            )
            self.lower_fb_name_modules = {
            t_name.lower(): t_class
            for t_name, t_class in flat_data_lib.__dict__.items()
            }
        except Exception as e:
            notice(
                f"Cannot import FlatData module. Make sure FlatData is available in Extracted folder. {e}",
                "error",
            )

    def _process_bytes_file(
        self, file_name: str, data: bytes
    ) -> tuple[dict[str, Any], str]:
        if not (
            flatbuffer_class := self.lower_fb_name_modules.get(
                file_name.removesuffix(".bytes").lower(), None
            )
        ):
            return {}, ""

        obj = None
        try:
            if flatbuffer_class.__name__.endswith("Table"):
                try:
                    if not file_name.endswith(".bytes") or not Config.is_cn:
                        data = xor_with_key(flatbuffer_class.__name__, data)
                    flat_buffer = getattr(flatbuffer_class, "GetRootAs")(data)
                    obj = getattr(self.dump_wrapper_lib, "dump_table")(flat_buffer)
                except:
                    pass

            if not obj:
                flat_buffer = getattr(flatbuffer_class, "GetRootAs")(data)
                obj = getattr(
                    self.dump_wrapper_lib, f"dump_{flatbuffer_class.__name__}"
                )(flat_buffer)
            return (obj, f"{flatbuffer_class.__name__}.json")
        except:
            return {}, ""

    def _process_json_file(self, data: bytes) -> bytes:
        try:
            data.decode("utf8")
            return data
        except:
            return bytes()

    def _process_db_file(self, file_path: str, table_name: str = "") -> list[DBTable]:
        with TableDatabase(file_path) as db:
            tables = []

            table_list = [table_name] if table_name else db.get_table_list()

            for table in table_list:
                columns = db.get_table_column_structure(table)
                rows: list[tuple] = db.get_table_data(table)[1]
                table_data = []
                for row in rows:
                    row_data: list[Any] = []
                    for col, value in zip(columns, row):
                        col_type = SQLiteDataType[col.data_type].value
                        if col_type == bytes:
                            data, _ = self._process_bytes_file(
                                table.replace("DBSchema", "Excel"), value
                            )
                            row_data.append(data)
                        elif col_type == bool:
                            row_data.append(bool(value))
                        else:
                            row_data.append(value)

                    table_data.append(row_data)
                tables.append(DBTable(table, columns, table_data))
            return tables

    def _process_zip_file(
        self,
        file_name: str,
        file_data: bytes,
        detect_type: bool = False,
    ) -> tuple[bytes, str, bool]:
        data = bytes()
        if (detect_type or file_name.endswith(".json")) and (
            data := self._process_json_file(file_data)
        ):
            return data, "", True

        if detect_type or file_name.endswith(".bytes"):
            b_data = self._process_bytes_file(file_name, file_data)
            file_dict, file_name = b_data
            if file_name:
                return (
                    json.dumps(file_dict, indent=4, ensure_ascii=False).encode("utf8"),
                    file_name,
                    True,
                )
        return data, "", False

    def extract_db_file(self, file_path: str) -> bool:
        """Extract db file."""
        try:
            if db_tables := self._process_db_file(
                file_path
            ):
                for table in db_tables:
                    db_extract_folder = self.extract_folder
                    os.makedirs(db_extract_folder, exist_ok=True)
                    with open(
                        path.join(db_extract_folder, f"{table.name.replace('DBSchema', 'Excel')}.json"),
                        "wt",
                        encoding="utf8",
                    ) as f:
                        json.dump(
                            TableDatabase.convert_to_list_dict(table),
                            f,
                            indent=4,
                            ensure_ascii=False,
                        )
                return True
            return False
        except Exception as e:
            print(f"Error when process {file_path}: {e}")
            return False

    def extract_zip_file(self, file_name: str) -> None:
        try:
            os.makedirs(self.extract_folder, exist_ok=True)

            password = zip_password(path.basename(file_name))
            with ZipFile(path.join(self.table_file_folder, file_name), "r") as zip:
                zip.setpassword(password)
                for item_name in zip.namelist():
                    item_data = zip.read(item_name)

                    data, name, success = bytes(), "", False
                    if item_name.endswith((".json", ".bytes")):
                        if "RootMotion" in file_name:
                            data, name, success = self._process_zip_file(
                                f"{file_name.removesuffix('.zip')}Flat", item_data, True
                            )
                            name = item_name
                        else:
                            data, name, success = self._process_zip_file(
                                item_name, item_data
                            )

                    if not success:
                        data, name, success = self._process_zip_file(
                            item_name, item_data, True
                        )
                    if success:
                        item_name = name if name else item_name
                        item_data = data
                    else:
                        notice(
                            f"The file {item_name} in {file_name} is not be implementate or cannot process."
                        )
                        continue

                    with open(path.join(self.extract_folder, item_name), "wb") as f:
                        f.write(item_data)
        except Exception as e:
            notice(f"Error when process {file_name}: {e}")

    def extract_table(self, file_path: str, decryption_key: str) -> None:
        if not file_path.endswith((".zip", ".db")):
            notice(f"The file {file_path} is not supported in current implementation.")
            return
    
        if file_path.endswith(".db"):            
            if Config.is_jp:
                if not Sqlcipher().decrypt_sqlcipher(path.join(self.table_file_folder, file_path), "./temp_ExcelDB.db", decryption_key):
                    notice(f"decrypt database {file_path} failed", "error")
                    return
                file_path = "./temp_ExcelDB.db"
            else:
                file_path = path.join(self.table_file_folder, file_path)
            self.extract_db_file(file_path)

        if file_path.endswith(".zip"):
            self.extract_zip_file(file_path)


class TableRepacker:
    def __init__(self, flat_data_module_name, db_key):
        try:
            self.db_key = db_key
            self.flat_data_lib = importlib.import_module(flat_data_module_name)
            self.repack_wrapper_lib = importlib.import_module(
                f"{flat_data_module_name}.repack_wrapper"
            )
        except Exception as e:
            notice(
                f"Cannot import FlatData module. Make sure FlatData is available in Extracted folder. {e}",
                "error",
            )

    def _normalize(self, s) -> None:
        if isinstance(s, str):
            return s.replace("‘", "'").replace("’", "'").replace("“", '"').replace("”", '"')
        return s

    def apply_replacements(self, input_filepath: Path, replacements_filepath: Path, skip_fields=[]) -> Path:
        with open(input_filepath, "r", encoding="utf8") as inp_f:
            data = json.loads(inp_f.read())
        with open(replacements_filepath, "r", encoding="utf8") as repl_f:
            replacements = json.loads(repl_f.read())
        print(f"loading replacements from {replacements_filepath}")
        unique_replacements = list({tuple(r["fields"]): r for r in replacements}.values())
        for repl_obj in unique_replacements:
            fields = repl_obj["fields"]
            mapping_list = repl_obj["mappings"]

            lookup_collection = defaultdict(lambda: defaultdict(list))
        
            for mapping in mapping_list:
                old_values = [self._normalize(item) for item in mapping["old"]]
                if "<?qi>" not in old_values:
                    used_fields = tuple(i for i, v in enumerate(old_values))
                else:
                    used_fields = tuple(i for i, v in enumerate(old_values) if v != "<?qi>")
                stripped_key = tuple(old_values[i] for i in used_fields)
            
                value = (
                    mapping["new"],
                    mapping.get("target_index", 0),
                    float(mapping.get("replacement_count", "inf"))
                )
                lookup_collection[used_fields][stripped_key].append(value)
        
            for struct in data:
                struct_values = [struct[field] for field in fields]
                for used_fields, lookup in lookup_collection.items():
                    key = tuple(self._normalize(struct_values[i]) for i in used_fields)
                    if key not in lookup:
                        continue
                    for i in range(len(lookup[key])):
                        new_values, target_index, replacement_count = lookup[key][i]
                        if target_index != 0:
                            lookup[key][i] = (new_values, target_index-1, replacement_count)
                            continue
                        if replacement_count > 0:
                            lookup[key][i] = (new_values, target_index, replacement_count-1)
                        else:
                            continue
                        for idx, field in enumerate(fields):
                            if field in skip_fields:
                                continue
                            if new_values[idx] == "<?skip>":
                                continue
                            struct[field] = new_values[idx]
        out_path = input_filepath.parent / "temp" / input_filepath.name
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(out_path, "wb") as out_f:
            out_f.write(json.dumps(data, ensure_ascii=False).encode())
            return out_path

    def repackExcel(self, json_path: Path):
        table_type = json_path.stem
        if not table_type:
            raise ValueError("JSON data must include a 'table' key indicating the table type.")
        pack_func_name = f"pack_{table_type}"
        pack_func = getattr(self.repack_wrapper_lib, pack_func_name, None)
        if not pack_func:
            raise ValueError(f"No pack function found for table type: {table_type}.")
        with open(json_path, 'r', encoding = 'utf-8') as f:
            json_data = json.loads(f.read())
            builder = flatbuffers.Builder(4096)
            offset = pack_func(builder, json_data)
            builder.Finish(offset)
            bytes_output = bytes(builder.Output())
            if not Config.is_cn:
                bytes_output = xor_with_key(table_type, bytes_output)
            return bytes_output

    def repackDB(self, json_path: Path, db_path_now: Path, db_path: Path) -> None:
        table_type = json_path.stem
        table_name = table_type.replace("Excel", "DBSchema")
        work_db_path = "./temp_ExcelDB.db"
        conn = sqlite3.connect(work_db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            conn.close()
            raise ValueError(f"Table '{table_name}' not found")
        
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        columns = [col[1] for col in columns_info]
        
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        cursor.execute(f"DELETE FROM {table_name};")
        
        placeholders = ', '.join(['?'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        try:
            for entry in json_data:
                builder = flatbuffers.Builder(4096)
                pack_func = getattr(self.repack_wrapper_lib, f"pack_{table_type}", None)
                if not pack_func:
                    raise ValueError(f"Pack function for {table_type} not found")
                
                offset = pack_func(builder, entry, False)
                builder.Finish(offset)
                bytes_output = bytes(builder.Output())
                
                flatbuffer_class = self.flat_data_lib.__dict__[table_type]
                flatbuffer_obj = getattr(flatbuffer_class, "GetRootAs")(bytes_output)
                
                row_values = []
                for col in columns:
                    if col == "Bytes":
                        row_values.append(bytes_output)
                    else:
                        if col not in entry:
                            raise ValueError(f"Missing field {col} in JSON entry")
                        
                        length_accessor = getattr(flatbuffer_obj, f"{col}Length", None)
                        item_accessor = getattr(flatbuffer_obj, col, None)
                        
                        if callable(item_accessor):
                            row_values.append([item_accessor(i) for i in range(length_accessor())] if callable(length_accessor) else item_accessor())
                        else:
                            raise ValueError(f"No valid accessor found for field '{col}'")
                
                cursor.execute(insert_query, row_values)
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Repacking failed: {str(e)}") from e
        
        finally:
            conn.close()

    def rebuild_database(self, db_path=Path("ExcelDB.db")):
        temp_path = "./temp_ExcelDB.db"
        conn = sqlite3.connect(temp_path)
        cursor = conn.cursor()
        new_conn = sqlite3.connect("temp.db")
        new_cursor = new_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        for table_name in tables:
            table_name = table_name[0]
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE name='{table_name}'")
            create_sql = cursor.fetchone()[0]
            new_cursor.execute(create_sql)
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            if rows:
                placeholders = ', '.join(['?'] * len(rows[0]))
                new_cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
        new_conn.commit()
        new_conn.close()
        print("Optimization complete!")
        Sqlcipher().encrypt_sqlcipher("temp.db", db_path, self.db_key)

class Sqlcipher:
    def decrypt_sqlcipher(self, encrypted_db_path: str, decrypted_db_path: str, decryption_key: str) -> bool:
         try:
             conn = sqlite.connect(encrypted_db_path)
             cursor = conn.cursor()
             cursor.execute(f"PRAGMA key = \"x'{decryption_key}'\";")
             cursor.execute("SELECT count(*) FROM sqlite_master;")
             print(f"数据库表数量: {cursor.fetchone()[0]}")
             new_conn = sqlite3.connect(decrypted_db_path)
             new_cursor = new_conn.cursor()
             cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
             tables = cursor.fetchall()
             
             print(f"找到 {len(tables)} 个用户表需要导出")
             for table in tables:
                 table_name = table[0]
                 print(f"正在导出表: {table_name}")
                 try:
                     cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
                     create_table_sql = cursor.fetchone()[0]
                     try:
                         new_cursor.execute(create_table_sql)
                     except sqlite.Error as e:
                         print(f"创建表 {table_name} 时出错: {e}")
                         if table_name in ['Group', 'Order', 'User', 'Table', 'Index', 'Select', 'Where']:
                             quoted_create_sql = create_table_sql.replace(table_name, f'"{table_name}"')
                             try:
                                 new_cursor.execute(quoted_create_sql)
                                 print(f"使用引号创建表 {table_name} 成功")
                             except sqlite.Error as e2:
                                 print(f"使用引号创建表 {table_name} 仍然失败: {e2}")
                                 continue
                         else:
                             continue
                     safe_table_name = f'"{table_name}"' if any(keyword in table_name.upper() for keyword in ['GROUP', 'ORDER', 'USER', 'TABLE', 'INDEX', 'SELECT', 'WHERE']) else table_name
                     
                     cursor.execute(f"SELECT * FROM {safe_table_name};")
                     rows = cursor.fetchall()
                     
                     if rows:
                         cursor.execute(f"PRAGMA table_info({safe_table_name});")
                         columns_info = cursor.fetchall()
                         column_names = [col[1] for col in columns_info]
                         
                         safe_column_names = [f'"{col}"' if any(keyword in col.upper() for keyword in ['GROUP', 'ORDER', 'USER']) else col for col in column_names]
                         
                         placeholders = ','.join(['?' for _ in column_names])
                         columns_str = ','.join(safe_column_names)
                         insert_sql = f"INSERT INTO {safe_table_name} ({columns_str}) VALUES ({placeholders})"
                         
                         try:
                             new_cursor.executemany(insert_sql, rows)
                             print(f"表 {table_name} 导出完成，共 {len(rows)} 行数据")
                         except sqlite.Error as e:
                             print(f"插入表 {table_name} 数据时出错: {e}")
                             for i, row in enumerate(rows):
                                 try:
                                     new_cursor.execute(insert_sql, row)
                                 except sqlite.Error as row_error:
                                     print(f"第 {i+1} 行插入失败: {row_error}")
                                     print(f"问题数据: {row}")
                                     break
                 
                 except Exception as table_error:
                     print(f"处理表 {table_name} 时发生未知错误: {table_error}")
                     continue
         
             new_conn.commit()
             new_conn.close()
             conn.close()
             
             print("解密成功！解密后的数据库保存在:", decrypted_db_path)
             return True
             
         except Exception as e:
             print(f"解密失败: {e}")
             return False

    def encrypt_sqlcipher(self, decrypted_db_path: str, encrypted_db_path: str, encryption_key: str) -> bool:
        try:
            source_conn = sqlite3.connect(f"file:{decrypted_db_path}?mode=ro", uri=True)
            source_cursor = source_conn.cursor()

            target_conn = sqlcipher3.connect(encrypted_db_path)
            target_cursor = target_conn.cursor()
            print("正在设置加密密钥...")
            target_cursor.execute(f"PRAGMA key = \"x'{encryption_key}'\";")
            target_cursor.execute("PRAGMA cipher_compatibility = 4;")
            target_cursor.execute("PRAGMA kdf_iter = 256000;")
            target_cursor.execute("PRAGMA cipher_page_size = 4096;")
            target_cursor.execute("PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
            target_cursor.execute("PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
        
            source_cursor.execute("""
                SELECT name, sql 
                FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sqlite_%'
                ORDER BY name;
            """)
            tables = source_cursor.fetchall()
        
            print(f"找到 {len(tables)} 个用户表")
        
            for table_name, create_sql in tables:
                if not create_sql:
                    continue
                
                print(f"创建表: {table_name}")
            
                try:
                    target_cursor.execute(create_sql)
                
                    source_cursor.execute(f'SELECT * FROM "{table_name}"')
                    rows = source_cursor.fetchall()
                
                    if rows:
                        source_cursor.execute(f'PRAGMA table_info("{table_name}")')
                        columns_info = source_cursor.fetchall()
                        column_names = [col[1] for col in columns_info]
                    
                        placeholders = ','.join(['?' for _ in column_names])
                        columns_str = ','.join([f'"{col}"' for col in column_names])
                        insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
                    
                        target_cursor.executemany(insert_sql, rows)
                        print(f"  → 插入 {len(rows)} 行数据")
                    else:
                        print(f"  → 表为空")
                    
                except Exception as e:
                    print(f"  ✗ 处理表 {table_name} 失败: {e}")
                    continue
        
            print("\n复制索引...")
            source_cursor.execute("""
                SELECT name, sql 
                FROM sqlite_master 
                WHERE type='index' 
                AND name NOT LIKE 'sqlite_%';
            """)
            indexes = source_cursor.fetchall()
        
            for index_name, create_sql in indexes:
                if not create_sql:
                    continue
                
                print(f"创建索引: {index_name}")
                try:
                    target_cursor.execute(create_sql)
                except Exception as e:
                    print(f"  ✗ 创建索引 {index_name} 失败: {e}")
        
            print("\n复制视图...")
            source_cursor.execute("""
                SELECT name, sql 
                FROM sqlite_master 
                WHERE type='view';
            """)
            views = source_cursor.fetchall()
        
            for view_name, create_sql in views:
                if not create_sql:
                    continue
                
                print(f"创建视图: {view_name}")
                try:
                    target_cursor.execute(create_sql)
                except Exception as e:
                    print(f"  ✗ 创建视图 {view_name} 失败: {e}")
        
            print("\n复制触发器...")
            source_cursor.execute("""
                SELECT name, sql 
                FROM sqlite_master 
                WHERE type='trigger';
            """)
            triggers = source_cursor.fetchall()
            
            for trigger_name, create_sql in triggers:
                if not create_sql:
                    continue
                
                print(f"创建触发器: {trigger_name}")
                try:
                    target_cursor.execute(create_sql)
                except Exception as e:
                    print(f"  ✗ 创建触发器 {trigger_name} 失败: {e}")
        
            target_conn.commit()
        
            target_conn.close()
            source_conn.close()
        
            return True
        
        except Exception as e:
            print(f"\n✗ 加密失败: {e}")
            import traceback
            traceback.print_exc()
            
            if os.path.exists(encrypted_db_path):
                try:
                    os.remove(encrypted_db_path)
                except:
                    pass
        
            return False
