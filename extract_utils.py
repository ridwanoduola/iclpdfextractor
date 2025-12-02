import ast
import io
import json
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed


def build_key_map(fields):
    key_map = {}
    for field in fields:
        f = field.strip()
        key_map[f.lower()] = f
        key_map[f.lower().replace(" ", "_")] = f
        key_map[f.lower().replace(" ", "")] = f
    return key_map


def replace_keywords_in_string(content, key_map):
    if not isinstance(content, str):
        return content
    result = content
    for key, value in key_map.items():
        result = result.replace(key, value)
    return result


def quick_clean_block(content):
    txns = []
    for line in content.split("\n"):
        s = line.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                txns.append(ast.literal_eval(s))
            except:
                pass
    return pd.DataFrame(txns)


def quick_clean_json(content):
    text = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
    blocks = re.findall(r'(\[.*?\])', text, re.DOTALL)

    txns = []
    for b in blocks:
        try:
            txns.extend(json.loads(b))
        except:
            pass
    return pd.DataFrame(txns)


def extract_html_tables(content):
    soup = BeautifulSoup(content, "html.parser")
    tables = soup.find_all("table")
    dfs = [pd.read_html(io.StringIO(str(t)))[0] for t in tables]
    return pd.concat(dfs, ignore_index=True)


def extract_all_data(content, fields):
    km = build_key_map(fields)
    new_content = replace_keywords_in_string(content, km)

    dfs = []

    if "<table" in new_content:
        dfs.append(extract_html_tables(new_content))

    if "[" in new_content and "{" in new_content:
        dfs.append(quick_clean_json(new_content))

    if "{" in new_content:
        dfs.append(quick_clean_block(new_content))

    dfs = [df for df in dfs if isinstance(df, pd.DataFrame)]

    if dfs:
        return pd.concat(dfs, ignore_index=True)

    return pd.DataFrame()


def extract_from_pdf_chunks_parallel(chunks, pages_data, api_key, max_workers=10):
    url = "https://extraction-api.nanonets.com/extract-async"
    headers = {"Authorization": f"Bearer {api_key}"}

    def process(idx, chunk_buf):
        files = {"file": (f"chunk_{idx}.pdf", chunk_buf, "application/pdf")}

        try:
            job = requests.post(url, files=files, data=pages_data, headers=headers).json()
            record_id = job["record_id"]

            poll_url = f"https://extraction-api.nanonets.com/files/{record_id}"

            while True:
                result = requests.get(poll_url, headers=headers).json()
                if result.get("processing_status") == "completed":
                    return result.get("content", "")
                if result.get("processing_status") in ("failed", "error"):
                    return ""
                time.sleep(7)

        except:
            return ""

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(process, i, c) for i, c in enumerate(chunks)]
        for f in as_completed(futs):
            results.append(f.result())

    return results
