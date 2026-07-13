#!/usr/bin/env python3
"""provisional_data.json → SQLite DB (prov_* 테이블) 마이그레이션

확정치(migrate_json.py)와 같은 패턴. 잠정치 정적 JSON을 API 뒤로 통일하기 위한
빌드 단계. Dockerfile에서 migrate_json 다음에 실행된다.

원본 구조:
    { 품목키: {h, d, u, s:{국가:{YYYYMM:{cut:{c,v,w,a}}}}} }
  - cut ∈ {'10','20','30'}
  - leaf c·a는 전 레코드 존재, v·w는 일부만 → 부재키는 NULL (0 채우기 금지)
  - 품목 순서(탭)·국가 순서(섹션 버튼)는 프론트가 Object.keys 순서에 의존 → sort_order로 보존
  - ym·cut 순서는 프론트가 항상 정렬하므로 무관
"""
import os, sys, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.config import DB_PATH, PROV_JSON_PATH
from server.database import init_db, get_connection


def _num(x):
    """숫자면 그대로, 그 외(None 포함)는 None → NULL 저장."""
    return x if isinstance(x, (int, float)) else None


def migrate():
    if not os.path.exists(PROV_JSON_PATH):
        print(f"ERROR: {PROV_JSON_PATH} 파일 없음")
        sys.exit(1)

    init_db()
    conn = get_connection()

    # prov 테이블은 매 빌드 전체 재구축 (정적 JSON이 단일 진실원본)
    conn.execute("DELETE FROM prov_data")
    conn.execute("DELETE FROM prov_countries")
    conn.execute("DELETE FROM prov_items")

    with open(PROV_JSON_PATH, "r", encoding="utf-8") as f:
        d = json.load(f)
    print(f"JSON 로드 완료: {os.path.getsize(PROV_JSON_PATH):,} bytes, 품목 {len(d)}개")

    n_items = n_countries = n_data = 0
    for item_order, (ikey, iv) in enumerate(d.items()):
        conn.execute(
            "INSERT OR REPLACE INTO prov_items VALUES (?,?,?,?,?)",
            (ikey, iv.get("h", ""), iv.get("d", ""), iv.get("u", ""), item_order))
        n_items += 1

        for c_order, (country, cv) in enumerate(iv.get("s", {}).items()):
            conn.execute(
                "INSERT OR REPLACE INTO prov_countries VALUES (?,?,?)",
                (ikey, country, c_order))
            n_countries += 1

            for ym, ymv in cv.items():
                for cut, leaf in ymv.items():
                    conn.execute(
                        "INSERT OR REPLACE INTO prov_data VALUES (?,?,?,?,?,?,?,?)",
                        (ikey, country, ym, cut,
                         _num(leaf.get("c")), _num(leaf.get("v")),
                         _num(leaf.get("w")), _num(leaf.get("a"))))
                    n_data += 1

    conn.commit()

    # ── 검증 요약 ──
    post_items = conn.execute("SELECT COUNT(*) FROM prov_items").fetchone()[0]
    post_ctry = conn.execute("SELECT COUNT(*) FROM prov_countries").fetchone()[0]
    post_data = conn.execute("SELECT COUNT(*) FROM prov_data").fetchone()[0]
    ymin, ymax = conn.execute("SELECT MIN(ym), MAX(ym) FROM prov_data").fetchone()
    nonnull = {col: conn.execute(
                   f"SELECT COUNT({col}) FROM prov_data").fetchone()[0]
               for col in ("c", "v", "w", "a")}
    print("\n=== 잠정치 마이그레이션 완료 ===")
    print(f"prov_items     {post_items:>6,}")
    print(f"prov_countries {post_ctry:>6,}")
    print(f"prov_data      {post_data:>6,}  ({ymin}~{ymax})")
    print(f"비-NULL leaf    c={nonnull['c']:,} v={nonnull['v']:,} "
          f"w={nonnull['w']:,} a={nonnull['a']:,}")
    print(f"DB 파일 크기: {os.path.getsize(DB_PATH):,} bytes")
    conn.close()


if __name__ == "__main__":
    migrate()
