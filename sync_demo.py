#!/usr/bin/env python3
"""trade_data_v2.json → trade.html DEMO 동기화"""
import os, json, re

base = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(base, "trade_data_v2.json")
html_path = os.path.join(base, "trade.html")

with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

demo_str = json.dumps(data, ensure_ascii=False)

with open(html_path, "r", encoding="utf-8") as f:
    html = f.read()

# const DEMO={...}; 패턴 교체
pattern = r'const DEMO=\{.*?\};'
replacement = f'const DEMO={demo_str};'

new_html, count = re.subn(pattern, replacement, html, count=1, flags=re.DOTALL)

if count == 0:
    print("ERROR: DEMO 패턴을 찾을 수 없습니다")
else:
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(new_html)
    print(f"DEMO 동기화 완료 ({os.path.getsize(html_path):,} bytes)")
