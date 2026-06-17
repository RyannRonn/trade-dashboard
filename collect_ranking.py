#!/usr/bin/env python3
"""
급등/급락 6자리 HS 전수 수집 (증분 방식)
- 4자리 HS 코드로 API 호출 → 6자리 hsCd 추출
- 이미 수집된 월은 건너뛰고 최신 월만 수집
- trade_data_v2.json의 "ranking_6d" 키에 저장
"""
import os, sys, json, time, io, sqlite3
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from customs_trade_v2 import api_call_xml, parse_ym_from_year, safe_int, REQUEST_DELAY

API_KEY = os.environ.get("API_KEY", "")

# 4자리 HS 코드 목록 파일
HS4_LIST_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "all_hs4.txt")


def get_all_hs4():
    """4자리 HS 코드 목록 로드"""
    with open(HS4_LIST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_months_to_collect(existing_months, target_months=14):
    """수집해야 할 월 목록 계산 (기존에 없는 월만)"""
    now = datetime.now()
    all_months = []
    y, m = now.year, now.month
    for _ in range(target_months):
        all_months.append(f"{y}{m:02d}")
        m -= 1
        if m < 1:
            m = 12
            y -= 1

    missing = [ym for ym in all_months if ym not in existing_months]
    return sorted(missing)


def make_ranges(months):
    """월 목록을 API 호출용 (start, end) 구간으로 변환 (최대 12개월씩)"""
    if not months:
        return []
    months = sorted(months)
    ranges = []
    i = 0
    while i < len(months):
        chunk = months[i:i+12]
        ranges.append((chunk[0], chunk[-1]))
        i += 12
    return ranges


def collect_hs4_batch(hs4, api_key, date_ranges):
    """4자리 HS 코드 1개 호출 → 6자리별 월별 수출액+중량+품목명 추출 + 국가별 분해.

    반환:
      items_6d:    {hs6: {"name": str, "exp": {ym: usd}, "wgt": {ym: kg}}}
      country_6d:  {hs6: {cd: {"name": kor, "exp": {ym: usd}, "wgt": {ym: kg}}}}
    """
    items_6d = defaultdict(lambda: {"name": "", "exp": {}, "wgt": {}})
    country_6d = defaultdict(lambda: defaultdict(lambda: {"name": "", "exp": {}, "wgt": {}}))

    for start, end in date_ranges:
        rows = api_call_xml("/nitemtrade/getNitemtradeList",
                            {"strtYymm": start, "endYymm": end, "hsSgn": hs4},
                            api_key)
        for r in rows:
            ym = parse_ym_from_year(r.get("year", ""))
            if not ym:
                continue
            hc = r.get("hsCd", "-").strip()
            if hc == "-" or len(hc) != 6:
                continue
            exp = safe_int(r.get("expDlr", 0))
            wgt = safe_int(r.get("expWgt", 0))
            items_6d[hc]["exp"][ym] = items_6d[hc]["exp"].get(ym, 0) + exp
            items_6d[hc]["wgt"][ym] = items_6d[hc]["wgt"].get(ym, 0) + wgt
            # 품목명: statKor 첫 번째 유효값 사용
            if not items_6d[hc]["name"]:
                nm = r.get("statKor", "").strip()
                if nm and nm != "-":
                    items_6d[hc]["name"] = nm
            # 국가별 분해 (statCd가 빈/대시면 스킵)
            cd = (r.get("statCd") or "").strip()
            if cd and cd != "-":
                slot = country_6d[hc][cd]
                slot["exp"][ym] = slot["exp"].get(ym, 0) + exp
                slot["wgt"][ym] = slot["wgt"].get(ym, 0) + wgt
                if not slot["name"]:
                    cnm = (r.get("statCdCntnKor1") or "").strip()
                    if cnm and cnm != "-":
                        slot["name"] = cnm

    # defaultdict(defaultdict(dict)) → 일반 dict로 변환 후 반환
    country_plain = {hc: {cd: dict(slot) for cd, slot in cmap.items()}
                     for hc, cmap in country_6d.items()}
    return dict(items_6d), country_plain


def init_db(db_path):
    """ranking_6d / ranking_6d_country 테이블 생성"""
    conn = sqlite3.connect(db_path)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS ranking_6d (
        hs_code TEXT NOT NULL,
        ym TEXT NOT NULL,
        name TEXT DEFAULT '',
        exp_usd INTEGER DEFAULT 0,
        wgt_kg INTEGER DEFAULT 0,
        PRIMARY KEY (hs_code, ym)
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_r6d_ym ON ranking_6d(ym)")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS ranking_6d_country (
        hs_code TEXT NOT NULL,
        ym TEXT NOT NULL,
        country_cd TEXT NOT NULL,
        country_nm TEXT DEFAULT '',
        exp_usd INTEGER DEFAULT 0,
        wgt_kg INTEGER DEFAULT 0,
        PRIMARY KEY (hs_code, ym, country_cd)
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_r6dc_ym ON ranking_6d_country(ym)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_r6dc_hs ON ranking_6d_country(hs_code)")
    # 기존 DB에 wgt_kg 컬럼이 없으면 추가 (마이그레이션 호환)
    for tbl in ("ranking_6d", "ranking_6d_country"):
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({tbl})")}
        if "wgt_kg" not in cols:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN wgt_kg INTEGER DEFAULT 0")
    conn.commit()
    return conn


def get_existing_months_from_db(conn):
    """DB에서 기존 수집된 월 목록 조회"""
    cur = conn.execute("SELECT DISTINCT ym FROM ranking_6d")
    return {row[0] for row in cur.fetchall()}


def save_batch_to_db(conn, batch, country_batch):
    """수집된 배치 데이터(HS6 합계 + HS6×국가)를 DB에 저장"""
    rows = []
    for hs6, info in batch.items():
        name = info.get("name", "")
        wgt_map = info.get("wgt", {})
        for ym, exp_usd in info.get("exp", {}).items():
            rows.append((hs6, ym, name, exp_usd, wgt_map.get(ym, 0)))
    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO ranking_6d (hs_code, ym, name, exp_usd, wgt_kg) VALUES (?, ?, ?, ?, ?)",
            rows
        )

    crows = []
    for hs6, cmap in (country_batch or {}).items():
        for cd, info in cmap.items():
            cnm = info.get("name", "")
            wgt_map = info.get("wgt", {})
            for ym, exp_usd in info.get("exp", {}).items():
                crows.append((hs6, ym, cd, cnm, exp_usd, wgt_map.get(ym, 0)))
    if crows:
        conn.executemany(
            "INSERT OR REPLACE INTO ranking_6d_country (hs_code, ym, country_cd, country_nm, exp_usd, wgt_kg) VALUES (?, ?, ?, ?, ?, ?)",
            crows
        )

    if rows or crows:
        conn.commit()
    return len(rows), len(crows)


def export_db_to_json(conn, json_path):
    """DB의 ranking_6d + ranking_6d_country → trade_data_v2.json 머지"""
    cur = conn.execute("SELECT hs_code, ym, name, exp_usd, wgt_kg FROM ranking_6d ORDER BY hs_code, ym")
    ranking = {}
    for hs_code, ym, name, exp_usd, wgt_kg in cur:
        if hs_code not in ranking:
            ranking[hs_code] = {"name": name, "exp": {}, "wgt": {}, "countries": {}}
        ranking[hs_code]["exp"][ym] = exp_usd
        ranking[hs_code]["wgt"][ym] = wgt_kg
        if not ranking[hs_code]["name"] and name:
            ranking[hs_code]["name"] = name

    cur2 = conn.execute("SELECT hs_code, ym, country_cd, country_nm, exp_usd, wgt_kg FROM ranking_6d_country ORDER BY hs_code, country_cd, ym")
    for hs_code, ym, cd, cnm, exp_usd, wgt_kg in cur2:
        if hs_code not in ranking:
            ranking[hs_code] = {"name": "", "exp": {}, "wgt": {}, "countries": {}}
        cmap = ranking[hs_code].setdefault("countries", {})
        if cd not in cmap:
            cmap[cd] = {"name": cnm or "", "exp": {}, "wgt": {}}
        cmap[cd]["exp"][ym] = exp_usd
        cmap[cd]["wgt"][ym] = wgt_kg
        if not cmap[cd]["name"] and cnm:
            cmap[cd]["name"] = cnm

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # 기존 .regions(시군구 데이터)는 별도 스크립트(collect_ranking_regions.py)가 채우므로 보존
    prev = data.get("ranking_6d", {})
    for hs, prev_v in prev.items():
        regs = prev_v.get("regions")
        if regs and hs in ranking:
            ranking[hs]["regions"] = regs
    data["ranking_6d"] = ranking
    from datetime import datetime
    data["ranking_generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    cnt_country = sum(len(v.get("countries", {})) for v in ranking.values())
    return len(ranking), cnt_country


def seed_db_from_json(conn, json_path):
    """커밋된 trade_data_v2.json의 ranking_6d를 DB에 시드.
    CI에서 trade.db가 없을 때(매번 빈 상태) 증분이 동작하도록 기존 월을 DB에 채운다."""
    if not os.path.exists(json_path):
        return 0
    try:
        data = json.load(open(json_path, encoding="utf-8"))
    except Exception:
        return 0
    rk = data.get("ranking_6d", {}) or {}
    rows, crows = [], []
    for hs6, info in rk.items():
        name = info.get("name", "")
        wgt = info.get("wgt", {}) or {}
        for ym, v in (info.get("exp", {}) or {}).items():
            rows.append((hs6, ym, name, v, wgt.get(ym, 0)))
        for cd, cinfo in (info.get("countries", {}) or {}).items():
            cnm = cinfo.get("name", "")
            cwgt = cinfo.get("wgt", {}) or {}
            for ym, v in (cinfo.get("exp", {}) or {}).items():
                crows.append((hs6, ym, cd, cnm, v, cwgt.get(ym, 0)))
    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO ranking_6d (hs_code, ym, name, exp_usd, wgt_kg) VALUES (?,?,?,?,?)", rows)
    if crows:
        conn.executemany(
            "INSERT OR REPLACE INTO ranking_6d_country (hs_code, ym, country_cd, country_nm, exp_usd, wgt_kg) VALUES (?,?,?,?,?,?)", crows)
    conn.commit()
    return len(rows)


def _recent_window(n):
    """현재월 기준 최근 n개월 YYYYMM 집합."""
    now = datetime.now()
    y, m = now.year, now.month
    out = set()
    for _ in range(n):
        out.add(f"{y}{m:02d}")
        m -= 1
        if m < 1:
            m = 12; y -= 1
    return out


def main():
    if not API_KEY:
        print("ERROR: API_KEY 환경변수 필요", file=sys.stderr)
        sys.exit(1)

    base = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base, "trade_data_v2.json")
    db_path = os.path.join(base, "trade.db")
    FULL_REBUILD = os.environ.get("FULL_REBUILD") == "1"
    RECENT_MONTHS = int(os.environ.get("RECENT_MONTHS", "3"))

    # DB 초기화 및 기존 수집 월 조회
    conn = init_db(db_path)
    existing_months = get_existing_months_from_db(conn)

    # CI에서 trade.db가 비어있으면 커밋된 JSON에서 시드 → 증분 가능
    if not existing_months and not FULL_REBUILD:
        seeded = seed_db_from_json(conn, json_path)
        if seeded:
            existing_months = get_existing_months_from_db(conn)
            print(f"trade.db 비어있음 → JSON에서 {seeded:,}행 시드 (기존월 {len(existing_months)}개)", flush=True)

    # revision 윈도우: 최근 N개월은 이미 있어도 다시 수집(확정치 소급수정 반영)
    if existing_months and not FULL_REBUILD:
        existing_months = existing_months - _recent_window(RECENT_MONTHS)

    # 수집할 월 계산
    missing = get_months_to_collect(existing_months)
    if not missing:
        print("모든 월이 이미 수집되어 있습니다. 종료.")
        conn.close()
        return

    print(f"기존 수집 월: {len(existing_months)}개", flush=True)
    print(f"신규 수집 월: {missing}", flush=True)

    date_ranges = make_ranges(missing)
    print(f"API 구간: {date_ranges}")

    # 4자리 HS 코드 목록
    hs4_list = get_all_hs4()
    total = len(hs4_list)
    print(f"\n4자리 HS 코드 {total}개 수집 시작...\n")

    new_count = 0
    total_rows = 0
    total_crows = 0
    start_time = time.time()
    WORKERS = int(os.environ.get("WORKERS", "5"))
    print(f"병렬 worker 수: {WORKERS}", flush=True)

    def _worker(hs4):
        return hs4, collect_hs4_batch(hs4, API_KEY, date_ranges)

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(_worker, hs4): hs4 for hs4 in hs4_list}
        for fut in as_completed(futures):
            try:
                hs4_done, (batch, country_batch) = fut.result()
            except Exception as e:
                print(f"  [ERR] {futures[fut]}: {e}", flush=True)
                done += 1
                continue
            if batch or country_batch:
                r, cr = save_batch_to_db(conn, batch, country_batch)
                total_rows += r
                total_crows += cr
                new_count += len(batch)
            done += 1
            if done % 100 == 0 or done == total:
                elapsed = time.time() - start_time
                pct = done / total * 100
                eta = elapsed / done * (total - done)
                print(f"  [{done}/{total}] {pct:.0f}% — {elapsed:.0f}s 경과, 잔여 {eta:.0f}s — HS6 {total_rows}행, 국가 {total_crows}행", flush=True)

    elapsed = time.time() - start_time
    print(f"\n수집 완료: {elapsed:.0f}초, HS6 {total_rows}행 + 국가 {total_crows}행 저장")

    # DB → JSON 내보내기
    hs6_count, country_count = export_db_to_json(conn, json_path)
    print(f"trade_data_v2.json 갱신 완료 (HS6 {hs6_count}개, 국가 슬롯 {country_count}개, {os.path.getsize(json_path):,} bytes)")

    conn.close()
    print("DONE")


if __name__ == "__main__":
    main()
