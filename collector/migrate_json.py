#!/usr/bin/env python3
"""trade_data_v2.json → SQLite DB 동기화 (누적형)

기존 DB를 유지한 채 새 JSON의 행을 INSERT OR REPLACE로 머지한다.
trade_data PK가 (data_type, hs_code, sub_code, entity_code, ym)이므로
같은 (HS·국가·월) 키만 덮어쓰고, 새 JSON에 없는 옛 달은 그대로 보존된다.
→ 매월 워크플로우가 14개월 롤링 윈도우만 수집해도 DB는 시간이 흐를수록 누적.
"""
import os, sys, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.config import DB_PATH, JSON_PATH
from server.database import init_db, get_connection


def migrate():
    if not os.path.exists(JSON_PATH):
        print(f"ERROR: {JSON_PATH} 파일 없음")
        sys.exit(1)

    db_existed = os.path.exists(DB_PATH)
    init_db()
    conn = get_connection()
    if db_existed:
        pre = conn.execute("SELECT COUNT(*) FROM trade_data").fetchone()[0]
        pre_min, pre_max = conn.execute(
            "SELECT MIN(ym), MAX(ym) FROM trade_data").fetchone()
        print(f"기존 trade.db 유지 · 현재 {pre:,}행 ({pre_min}~{pre_max}) → 머지 시작")
    else:
        print("trade.db 신규 생성")

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        d = json.load(f)

    print(f"JSON 로드 완료: {os.path.getsize(JSON_PATH):,} bytes")

    # ── 1) meta ──
    conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                 ("generated_at", d.get("generated_at", "")))
    conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                 ("period_start", d.get("period", {}).get("start", "")))
    conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                 ("period_end", d.get("period", {}).get("end", "")))
    conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                 ("main_items", json.dumps(d.get("main_items", []), ensure_ascii=False)))
    conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                 ("sub_items_def", json.dumps(d.get("sub_items_def", {}), ensure_ascii=False)))
    print("  meta 완료")

    # ── 2) countries, regions ──
    for code, name in d.get("all_countries", {}).items():
        conn.execute("INSERT OR REPLACE INTO countries VALUES (?,?)", (code, name))
    for code, name in d.get("all_regions", {}).items():
        conn.execute("INSERT OR REPLACE INTO regions VALUES (?,?)", (code, name))
    print(f"  countries {len(d.get('all_countries', {}))}개, regions {len(d.get('all_regions', {}))}개")

    # ── 3) hs_names ──
    hs_count = 0
    for code, name in d.get("hs2_names", {}).items():
        conn.execute("INSERT OR REPLACE INTO hs_names VALUES (?,?,2)", (code, name))
        hs_count += 1
    for code, name in d.get("hs4_names", {}).items():
        conn.execute("INSERT OR REPLACE INTO hs_names VALUES (?,?,4)", (code, name))
        hs_count += 1
    print(f"  hs_names {hs_count}개 (2자리+4자리)")

    # ── 4) total ──
    total_exp = d.get("total", {}).get("exp", {})
    total_imp = d.get("total", {}).get("imp", {})
    all_ym = set(total_exp.keys()) | set(total_imp.keys())
    for ym in all_ym:
        conn.execute(
            "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
            ("total", "", "", "", ym, total_exp.get(ym, 0), total_imp.get(ym, 0), 0))
    print(f"  total {len(all_ym)}개월")

    # ── 5) items ──
    main_items = d.get("main_items", [])
    td_count = len(all_ym)  # total 카운트 시작

    for hs, item in d.get("items", {}).items():
        is_main = 1 if hs in main_items else 0
        sort_order = main_items.index(hs) if hs in main_items else 999
        conn.execute("INSERT OR REPLACE INTO items VALUES (?,?,?,?)",
                     (hs, item.get("name", hs), is_main, sort_order))

        # item 총계
        t_exp = item.get("total_exp", {})
        t_imp = item.get("total_imp", {})
        t_wgt = item.get("total_wgt", {})
        for ym in set(t_exp.keys()) | set(t_imp.keys()):
            conn.execute(
                "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
                ("item", hs, "", "", ym,
                 t_exp.get(ym, 0), t_imp.get(ym, 0), t_wgt.get(ym, 0)))
            td_count += 1

        # 국가별
        for cd, cdata in item.get("countries", {}).items():
            conn.execute("INSERT OR IGNORE INTO item_countries VALUES (?,?)", (hs, cd))
            for ym, val in cdata.get("exp", {}).items():
                wgt = cdata.get("wgt", {}).get(ym, 0)
                conn.execute(
                    "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
                    ("item_country", hs, "", cd, ym, val, 0, wgt))
                td_count += 1

        # 지역별
        for rcode, rdata in item.get("regions", {}).items():
            for ym, val in rdata.get("exp", {}).items():
                conn.execute(
                    "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
                    ("item_region", hs, "", rcode, ym, val, 0, 0))
                td_count += 1

        # 세부항목
        for scode, sdata in item.get("sub_items", {}).items():
            sname = sdata.get("name", scode)
            conn.execute("INSERT OR REPLACE INTO sub_items VALUES (?,?,?)",
                         (hs, scode, sname))
            # hs_names에도 등록
            conn.execute("INSERT OR REPLACE INTO hs_names VALUES (?,?,?)",
                         (scode, sname, len(scode)))
            hs_count += 1

            for ym, val in sdata.get("exp", {}).items():
                wgt = sdata.get("wgt", {}).get(ym, 0)
                conn.execute(
                    "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
                    ("sub_item", hs, scode, "", ym, val, 0, wgt))
                td_count += 1

            # 세부항목 국가별
            for cd, cdata in sdata.get("countries", {}).items():
                for ym, val in cdata.get("exp", {}).items():
                    wgt = cdata.get("wgt", {}).get(ym, 0)
                    conn.execute(
                        "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
                        ("sub_country", hs, scode, cd, ym, val, 0, wgt))
                    td_count += 1

        # 기업별
        for ck, cdata in item.get("companies", {}).items():
            conn.execute("INSERT OR REPLACE INTO companies VALUES (?,?,?)",
                         (hs, ck, cdata.get("name", ck)))
            for lk, ldata in cdata.get("locations", {}).items():
                conn.execute(
                    "INSERT OR REPLACE INTO company_locations VALUES (?,?,?,?)",
                    (hs, ck, lk, ldata.get("name", lk)))
                for ym, val in ldata.get("exp", {}).items():
                    conn.execute(
                        "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
                        ("company_loc", hs, ck, lk, ym, val, 0, 0))
                    td_count += 1

        # samyang (1902 전용)
        if "samyang" in item:
            conn.execute("INSERT OR REPLACE INTO companies VALUES (?,?,?)",
                         (hs, "samyang", "삼양식품"))
            for lk, ldata in item["samyang"].items():
                conn.execute(
                    "INSERT OR REPLACE INTO company_locations VALUES (?,?,?,?)",
                    (hs, "samyang", lk, ldata.get("name", lk)))
                for ym, val in ldata.get("exp", {}).items():
                    conn.execute(
                        "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
                        ("company_loc", hs, "samyang", lk, ym, val, 0, 0))
                    td_count += 1

    print(f"  items {len(d.get('items', {}))}개")

    # ── 6) ranking_6d ──
    rk_count = 0
    for hs6, rdata in d.get("ranking_6d", {}).items():
        rname = rdata.get("name", "")
        if rname:
            conn.execute("INSERT OR REPLACE INTO hs_names VALUES (?,?,?)",
                         (hs6, rname, len(hs6)))
        for ym, val in rdata.get("exp", {}).items():
            conn.execute(
                "INSERT OR REPLACE INTO trade_data VALUES (?,?,?,?,?,?,?,?)",
                ("ranking", "", hs6, "", ym, val, 0, 0))
            td_count += 1
            rk_count += 1
    print(f"  ranking_6d {len(d.get('ranking_6d', {}))}개 항목, {rk_count}개 데이터포인트")

    conn.commit()

    # ── 검증 ── (DB 실제 행수 기준 / td_count는 upsert 호출 횟수라 누적 머지 시 부정확)
    post = conn.execute("SELECT COUNT(*) FROM trade_data").fetchone()[0]
    post_min, post_max = conn.execute(
        "SELECT MIN(ym), MAX(ym) FROM trade_data").fetchone()
    print(f"\n=== 머지 완료 ===")
    print(f"trade_data 실제 행수: {post:,} ({post_min}~{post_max})")
    print(f"이번 실행 upsert 호출수: {td_count:,}")
    for row in conn.execute(
            "SELECT data_type, COUNT(*) as cnt FROM trade_data GROUP BY data_type ORDER BY cnt DESC"):
        print(f"  {row['data_type']:15s} {row['cnt']:>8,}")
    print(f"DB 파일 크기: {os.path.getsize(DB_PATH):,} bytes")

    conn.close()


if __name__ == "__main__":
    migrate()
