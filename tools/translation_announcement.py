import json
import os
import re
import requests
import logging
import boto3
import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup
from openai import OpenAI
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
from utils.util import R2Transfer, KVTransfer

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

JSON_PATH = "prod/index.json"
NOUN_FILE = "other/translation_noun.txt"
API_KEY = os.getenv("DEEPSEEK_API_KEY")

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
KV_INDEX_NAMESPACE = "5011cc6edebc4aa69618fd744690e22e"
KV_HTML_NAMESPACE = "cf4cc49fd54d489aa61a30502a0150d7"

print(f"[{get_now()}] [初始化] API_KEY: {'已加载' if API_KEY else '未找到'}")
print(f"[{get_now()}] [初始化] CF_ACCOUNT_ID: {'已加载' if CF_ACCOUNT_ID else '未找到'}")

NEW_CSS = """
  .news-title { font-family: sans-serif; font-weight: bold; font-size: 2rem; border-bottom: 1px solid #00ccff; margin-top: 1rem; }
  .news-time { text-align: right; font-size: 1rem; }
  .news-image { width: 100%; }
  img { width: 100%; border-radius: 8px; }
  p { font-family: sans-serif; font-size: 1.5rem; line-height: 1.6; color: #444444; }
"""

client = OpenAI(api_key=API_KEY, base_url="https://api.deepseek.com")
kv_client = KVTransfer(account_id=CF_ACCOUNT_ID, api_token=CF_API_TOKEN)

def load_noun_table():
    try:
        print(f"[{get_now()}] [名词表] 正在读取: {NOUN_FILE}")
        with open(NOUN_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            print(f"[{get_now()}] [名词表] 读取成功，长度: {len(content)}")
            return content
    except Exception as e:
        print(f"[{get_now()}] [名词表] 读取失败或文件不存在: {e}")
        return ""

def translate_single(text, noun_table):
    print(f"[{get_now()}] [API 单条翻译] 正在翻译短句: {text[:20]}...")
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": f"你是一个专业的游戏翻译。名词表：\n{noun_table}"},
                {"role": "user", "content": f"请将以下日文翻译为中文，直接返回结果：\n{text}"}
            ],
            timeout=15
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"[{get_now()}] [API 异常] 单条翻译失败: {e}")
        return text

def translate_batch(texts, noun_table):
    if not texts:
        return []
    indices = [i for i, t in enumerate(texts) if t and t.strip() and not re.match(r'^[0-9\s\W]+$', t)]
    if not indices:
        return texts
    
    print(f"[{get_now()}] [API 批量翻译] 提交数量: {len(indices)}/{len(texts)}")
    to_translate_map = [{"id": i, "content": texts[i]} for i in indices]
    translated_results = list(texts)
    
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": f"你是一个专业的游戏翻译。名词表：\n{noun_table}\n输入是一个包含id和content的JSON数组。请返回相同结构的JSON数组，content替换为中文翻译。必须严格保持id对应，严禁合并条目。"},
                {"role": "user", "content": json.dumps(to_translate_map, ensure_ascii=False)},
            ],
            temperature=0,
            timeout=60
        )
        raw_content = response.choices[0].message.content.strip()
        if raw_content.startswith("```"):
            raw_content = re.sub(r'^```(?:json)?\n?|```$', '', raw_content, flags=re.MULTILINE).strip()
        
        data = json.loads(raw_content)
        items = data if isinstance(data, list) else next(iter([v for v in data.values() if isinstance(v, list)]), [])
        
        translated_ids = set()
        for item in items:
            try:
                curr_id = int(item.get("id"))
                curr_content = item.get("content")
                if curr_id in indices:
                    translated_results[curr_id] = curr_content
                    translated_ids.add(curr_id)
            except:
                continue
        
        print(f"[{get_now()}] [API 批量翻译] 成功收回: {len(translated_ids)} 条")
        
        missing_ids = set(indices) - translated_ids
        if missing_ids:
            print(f"[{get_now()}] [补漏] 缺失 {len(missing_ids)} 条，开始逐条翻译...")
            for mid in missing_ids:
                translated_results[mid] = translate_single(texts[mid], noun_table)
    except Exception as e:
        print(f"[{get_now()}] [API 异常] 批量翻译出错: {e}")
    return translated_results

def translate_html_content(html_path, noun_table):
    print(f"[{get_now()}] [HTML 进程] 正在解析: {html_path}")
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
        
        if soup.style:
            soup.style.string = NEW_CSS
        else:
            style_tag = soup.new_tag("style", type="text/css")
            style_tag.string = NEW_CSS
            if soup.head: soup.head.append(style_tag)

        nodes = [n for n in soup.find_all(string=True) 
                if n.parent.name not in ['style', 'script', 'meta', 'link'] and n.strip()]
        
        if nodes:
            original_texts = [str(n).strip() for n in nodes]
            translated_texts = translate_batch(original_texts, noun_table)
            for node, t_text in zip(nodes, translated_texts):
                node.replace_with(t_text)
        
        final_html = str(soup)
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(final_html)
        
        print(f"[{get_now()}] [KV 存储] 正在上传 HTML: {html_path}")
        kv_client.put_value(KV_HTML_NAMESPACE, html_path, final_html)
    except Exception as e:
        print(f"[{get_now()}] [HTML 错误] {html_path}: {e}")

def single_page_task(item, noun_table):
    url = item.get("Url")
    if not url: return

    if "[https://prod-notice.bluearchiveyostar.com](https://prod-notice.bluearchiveyostar.com)" in url and "index" in url:
        url = url.replace("[https://prod-notice.bluearchiveyostar.com](https://prod-notice.bluearchiveyostar.com)", "[https://prod-notice.bluearchive.cafe](https://prod-notice.bluearchive.cafe)")
    
    path_match = re.search(r'prod/(\d+)/(\d+)/index(\d+).html', url)
    if path_match:
        local_path = f"prod/{path_match.group(1)}/{path_match.group(2)}/index{path_match.group(3)}.html"
    else:
        path_match_alt = re.search(r'prod/(GachaProbabilityDisplay/\d+/index\.html)', url)
        if path_match_alt:
            local_path = f"prod/{path_match_alt.group(1)}"
        else:
            return

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        print(f"[{get_now()}] [网络下载] 正在请求: {url}")
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(res.content)
        translate_html_content(local_path, noun_table)
    except Exception as e:
        print(f"[{get_now()}] [处理失败] {url}: {e}")

def download_and_translate_pages(data, noun_table):
    targets = ["GachaProbabilityDisplay", "Issues", "Events", "Notices"]
    all_items = []
    for key in targets:
        if key in data:
            all_items.extend(data[key])
    
    if not all_items:
        print(f"[{get_now()}] [任务] 没有发现需要处理的页面。")
        return
    
    print(f"[{get_now()}] [并发开始] 准备处理 {len(all_items)} 个页面 (线程数: 5)")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(single_page_task, item, noun_table) for item in all_items]
        for _ in tqdm(as_completed(futures), total=len(all_items), desc="处理页面"):
            pass

def collect_json_texts(node, targets, results):
    if isinstance(node, dict):
        for k, v in node.items():
            if k in targets and isinstance(v, str) and v.strip():
                results.append((node, k, v))
            else:
                collect_json_texts(v, targets, results)
    elif isinstance(node, list):
        for item in node:
            collect_json_texts(item, targets, results)

def update_urls_in_json(node):
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "Url" and isinstance(v, str):
                if "[https://prod-notice.bluearchiveyostar.com](https://prod-notice.bluearchiveyostar.com)" in v and "index" in v:
                    node[k] = v.replace("[https://prod-notice.bluearchiveyostar.com](https://prod-notice.bluearchiveyostar.com)", "[https://prod-notice.bluearchive.cafe](https://prod-notice.bluearchive.cafe)")
            else:
                update_urls_in_json(v)
    elif isinstance(node, list):
        for item in node:
            update_urls_in_json(item)

def main():
    print(f"[{get_now()}] >>> 脚本启动 <<<")
    try:
        print(f"[{get_now()}] [文件读取] 加载主索引: {JSON_PATH}")
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[{get_now()}] [致命错误] 读取 JSON 失败: {e}")
        return

    noun_table = load_noun_table()
    
    print(f"[{get_now()}] [JSON 翻译] 正在扫描待翻译文本域...")
    json_tasks = []
    collect_json_texts(data, {"Message", "PopupOKText", "Text", "Title"}, json_tasks)
    
    if json_tasks:
        print(f"[{get_now()}] [JSON 翻译] 共发现 {len(json_tasks)} 条文本")
        batch_size = 15
        for i in tqdm(range(0, len(json_tasks), batch_size), desc="翻译 JSON"):
            batch = json_tasks[i:i+batch_size]
            results = translate_batch([item[2] for item in batch], noun_table)
            for (node, key, _), res_text in zip(batch, results):
                node[key] = res_text
    
    print(f"[{get_now()}] [JSON 更新] 正在修复 URL 链接...")
    update_urls_in_json(data)
    
    print(f"[{get_now()}] [文件保存] 正在回写 JSON: {JSON_PATH}")
    final_json_str = json.dumps(data, ensure_ascii=False, indent=2)
    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        f.write(final_json_str)
    
    print(f"[{get_now()}] [KV 存储] 正在上传主索引到 Cloudflare...")
    kv_client.put_value(KV_INDEX_NAMESPACE, "prod/index.json", final_json_str)

    print(f"[{get_now()}] [页面处理] 开始下载并翻译 HTML 详情页...")
    download_and_translate_pages(data, noun_table)
    
    print(f"[{get_now()}] >>> 任务全部完成 <<<")

if __name__ == "__main__":
    main()
