#!/usr/bin/env python3
"""trade_data_v2.json의 ranking_6d → trade.db ranking_6d 테이블 마이그레이션"""
import os, json, sqlite3

base = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(base, "trade_data_v2.json")
db_path = os.path.join(base, "trade.db")

# DB 테이블 생성
conn = sqlite3.connect(db_path)
conn.execute("""
CREATE TABLE IF NOT EXISTS ranking_6d (
    hs_code TEXT NOT NULL,
    ym TEXT NOT NULL,
    name TEXT DEFAULT '',
    exp_usd INTEGER DEFAULT 0,
    PRIMARY KEY (hs_code, ym)
)
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_r6d_ym ON ranking_6d(ym)")
conn.commit()

# JSON 로드
with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

ranking = data.get("ranking_6d", {})
if not ranking:
    print("ranking_6d 데이터가 없습니다.")
    exit(1)

# INSERT OR REPLACE
rows = []
for hs_code, info in ranking.items():
    name = info.get("name", "")
    for ym, exp_usd in info.get("exp", {}).items():
        rows.append((hs_code, ym, name, exp_usd))

conn.executemany(
    "INSERT OR REPLACE INTO ranking_6d (hs_code, ym, name, exp_usd) VALUES (?, ?, ?, ?)",
    rows
)
conn.commit()
conn.close()

print(f"마이그레이션 완료: {len(ranking)}개 HS6 코드, {len(rows)}개 행 삽입")
