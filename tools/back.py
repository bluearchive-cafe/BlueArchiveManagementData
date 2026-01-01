import json
import sys
from pathlib import Path
import argparse

def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to load JSON: {path}\nError: {str(e)}")
        raise

def save_json(path, data):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Failed to save JSON: {path}\nError: {str(e)}")
        raise

def get_cn_field(jp_field):
    if 'JP' in jp_field:
        return jp_field.replace('JP', 'CN')
    elif 'Jp' in jp_field:
        return jp_field.replace('Jp', 'Cn')
    elif 'jp' in jp_field:
        return jp_field.replace('jp', 'cn')
    return jp_field

def get_re_key(jp_key: str) -> str:
    return jp_key.replace("Jp", "Re").replace("jp", "re").replace("JP", "RE")

def get_tr_key(kr_key: str) -> str:
    return kr_key.replace("Kr", "Tr").replace("kr", "tr").replace("KR", "TR")

def is_jp_field(field_name):
    jp_indicators = ['JP', 'Jp', 'jp', 'Japanese']
    return any(indicator in field_name for indicator in jp_indicators)

def is_kr_field(field_name):
    kr_indicators = ['Kr', 'kr', 'KR', 'Korean', 'ScriptKr']
    return any(indicator in field_name for indicator in kr_indicators)

def is_cn_field(field_name):
    cn_indicators = ['CN', 'Cn', 'cn', 'Chinese']
    return any(indicator in field_name for indicator in cn_indicators)

def is_re_field(field_name):
    re_indicators = ['RE', 'Re', 're']
    return any(indicator in field_name for indicator in re_indicators)

def is_tr_field(field_name):
    tr_indicators = ['TR', 'Tr', 'tr']
    return any(indicator in field_name for indicator in tr_indicators)

def should_remove_field(field_name, keep_fields):
    """判断字段是否应该被删除"""
    if field_name in keep_fields:
        return False
    
    # 强制删除所有CN、RE、TR后缀的键
    if is_cn_field(field_name) or is_re_field(field_name) or is_tr_field(field_name):
        return True
    
    return False

def replace_jp_with_cn(modified_dir, config_path):
    modified_dir = Path(modified_dir)
    print("Starting JP to CN text replacement...")
    
    try:
        cfg = load_json(config_path)
        processed_files = 0
        replaced_items = 0
        skipped_empty = 0

        for schema_type, schema in cfg.items():
            for filename, fields in schema.items():
                if not fields or len(fields) < 2:
                    continue

                source_files = [
                    modified_dir / "NewBack" / schema_type / filename,
                    modified_dir / "ReviseBack" / schema_type / filename
                ]
                
                source_file = None
                for f in source_files:
                    if f.exists():
                        source_file = f
                        break
                
                if source_file is None:
                    print(f"File not found: {filename}")
                    continue
                
                source_data = load_json(source_file)
                if not source_data:
                    print(f"Empty data: {filename}")
                    continue
                    
                key_field = fields[0]
                
                jp_fields = [field for field in fields[1:] if is_jp_field(field)]
                
                if not jp_fields:
                    print(f"No JP fields found in {filename}, skipping")
                    continue
                
                print(f"\nProcessing {filename}:")
                print(f"Key field: {key_field}")
                print(f"JP fields: {jp_fields}")
                print(f"Original fields in data: {list(source_data[0].keys())}")

                file_replaced = 0
                file_skipped = 0
                
                for item in source_data:
                    for jp_field in jp_fields:
                        cn_field = get_cn_field(jp_field)
                        re_field = get_re_key(jp_field)
                        
                        # 优先使用CN字段，如果没有CN则使用Re字段
                        source_text = None
                        if cn_field in item and item[cn_field]:
                            source_text = item[cn_field]
                        elif re_field in item and item[re_field]:
                            source_text = item[re_field]
                        
                        if source_text is not None:
                            jp_text = item.get(jp_field, "")
                            
                            if source_text != jp_text:
                                item[jp_field] = source_text
                                file_replaced += 1
                            else:
                                file_skipped += 1
                        else:
                            file_skipped += 1

                # 确定要保留的字段：JP字段、KR字段、关键字段、VoiceId
                keep_fields = set(jp_fields)
                keep_fields.add(key_field)
                keep_fields.add('VoiceId')
                
                # 添加所有KR字段到保留列表
                for field in list(source_data[0].keys()):
                    if is_kr_field(field):
                        keep_fields.add(field)
                
                print(f"Fields to keep: {sorted(keep_fields)}")
                
                # 删除所有CN、RE、TR字段（除了要保留的字段）
                fields_to_remove = []
                for field in list(source_data[0].keys()):
                    if should_remove_field(field, keep_fields):
                        fields_to_remove.append(field)
                
                print(f"Fields to remove: {fields_to_remove}")
                
                # 执行删除操作
                for item in source_data:
                    for field in fields_to_remove:
                        if field in item:
                            del item[field]
                
                remaining_fields = list(source_data[0].keys()) if source_data else []
                print(f"Remaining fields after processing: {remaining_fields}")
                
                save_json(source_file, source_data)
                
                processed_files += 1
                replaced_items += file_replaced
                skipped_empty += file_skipped

                print(f"  - Replaced: {file_replaced}, Skipped: {file_skipped}")

        print("\nProcessing summary:")
        print(f"Total files processed: {processed_files}")
        print(f"Total items replaced: {replaced_items}")
        print(f"Total items skipped (no change needed): {skipped_empty}")

        return processed_files, replaced_items

    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='JP to CN text replacement tool')
    parser.add_argument('config_path', help='Path to configuration JSON')
    parser.add_argument('modified_dir', help='Directory containing NewBack and ReviseBack folders')
    args = parser.parse_args()

    try:
        processed, replaced = replace_jp_with_cn(
            Path(args.modified_dir),
            Path(args.config_path)
        )
        if replaced == 0:
            print("No replacements were made")
    except Exception as e:
        print(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    main()
