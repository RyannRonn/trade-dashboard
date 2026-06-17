#!/usr/bin/env python3
"""한국 전체 수출입 총액 수집 (HS2 99개 합산)

nitemtrade API의 hsSgn=2자리 호출 시 응답 첫 행 year='총계'에 그 HS2 챕터의
한국 전체 합계가 옴. 99개 HS2(01~99) 합산 = 한국 전체 수출입 총액.

이전엔 customs_trade_v2.py가 16개 모니터링 품목 합을 'total'로 저장해서
대시보드에 한국 전체로 잘못 표시됨 (수출 50%, 수입 28% 수준).
"""
import os, sys, io, sqlite3, json, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from customs_trade_v2 import api_call_xml, parse_ym_from_year, safe_int, get_incremental_ranges

API_KEY = os.environ.get("API_KEY", "")
TARGET_MONTHS = 14
WORKERS = int(os.environ.get("WORKERS", "5"))
FULL_REBUILD = os.environ.get("FULL_REBUILD") == "1"


def get_target_months():
    from datetime import datetime
    now = datetime.now()
    months = []
    y, m = now.year, now.month
    for _ in range(TARGET_MONTHS):
        months.append(f"{y}{m:02d}")
        m -= 1
        if m < 1:
            m = 12; y -= 1
    return sorted(months)


def make_ranges(months):
    ranges = []
    i = 0
    while i < len(months):
        chunk = months[i:i+12]
        ranges.append((chunk[0], chunk[-1]))
        i += 12
    return ranges


def collect_hs2(hs2, api_key, date_ranges):
    """HS2 1개의 월별 한국 전체 수출입 합계 (응답 첫 행 '총계' 사용)"""
    exp = defaultdict(int)
    imp = defaultdict(int)
    for start, end in date_ranges:
        rows = api_call_xml("/nitemtrade/getNitemtradeList",
                            {"strtYymm": start, "endYymm": end, "hsSgn": hs2},
                            api_key)
        # 응답: ym별로 첫 행이 year='총계'. 그 행의 expDlr/impDlr이 그 ym의 HS2 전체합
        # 단 API는 ym별 '총계' 하나만 옴(전체기간 총계 아님). 검증: 값 = 다른 행 합과 일치
        for r in rows:
            if r.get("year") != "총계":
                continue
            # '총계' 행은 ym 정보 없음 → 그 호출의 모든 ym에 적용 불가
            # 실제로는 호출 1회 = 단일 ym(start==end)일 때만 정확
            pass
        # 행 합산 방식이 더 정확/안전
        for r in rows:
            if r.get("year") == "총계":
                continue
            ym = parse_ym_from_year(r.get("year", ""))
            if not ym:
                continue
            exp[ym] += safe_int(r.get("expDlr", 0))
            imp[ym] += safe_int(r.get("impDlr", 0))
    return dict(exp), dict(imp)


def main():
    if not API_KEY:
        print("ERROR: API_KEY 환경변수 필요", file=sys.stderr); sys.exit(1)

    base = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base, "trade_data_v2.json")
    db_path = os.path.join(base, "trade.db")

    # 증분: 기존 JSON total에 든 월 기준 최근 N개월만(없으면 14개월 전체)
    ranges, is_full = get_incremental_ranges(json_path)
    months = []
    for s, e in ranges:
        cur = s
        while cur <= e:
            months.append(cur)
            yy, mm = int(cur[:4]), int(cur[4:]); mm += 1
            if mm > 12: mm = 1; yy += 1
            cur = f"{yy}{mm:02d}"
    months = sorted(months)
    mode = "전체(14개월)" if is_full else f"증분(최근 {len(months)}개월)"
    print(f"수집 모드: {mode} — {months[0]}~{months[-1]}, 구간 {ranges}")

    hs2_list = [f"{i:02d}" for i in range(1, 100)]  # 01~99
    print(f"HS2 {len(hs2_list)}개 호출 (worker={WORKERS})...")

    exp_total = defaultdict(int)
    imp_total = defaultdict(int)
    start_time = time.time()

    def _worker(hs2):
        return hs2, collect_hs2(hs2, API_KEY, ranges)

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(_worker, h): h for h in hs2_list}
        for fut in as_completed(futures):
            try:
                hs2, (exp, imp) = fut.result()
            except Exception as e:
                print(f"  [ERR] {futures[fut]}: {e}"); done += 1; continue
            for ym, v in exp.items(): exp_total[ym] += v
            for ym, v in imp.items(): imp_total[ym] += v
            done += 1
            if done % 20 == 0 or done == len(hs2_list):
                el = time.time() - start_time
                print(f"  [{done}/{len(hs2_list)}] {el:.0f}s")

    print(f"\n수집 완료: {time.time()-start_time:.0f}초")
    print("월별 합계:")
    for ym in sorted(exp_total.keys()):
        print(f"  {ym}: exp={exp_total[ym]:>15,} imp={imp_total[ym]:>15,}")

    # ── 안전장치: 수집 실패/미집계 월 감지 ──
    # 한국 월간 수출은 최소 $20B 수준. 그보다 작은 월은 수집 실패(API 쿼터
    # 소진/LOCK) 또는 미집계(아직 안 끝난 당월)이므로 제외한다.
    # 정상 월이 하나도 없으면 total을 건드리지 않고 비정상 종료해서
    # workflow가 눈에 띄게 실패하도록 한다 (조용히 garbage를 쓰지 않음).
    SANITY_MIN_EXP = 20_000_000_000  # $20B
    valid = sorted(ym for ym in exp_total if exp_total[ym] >= SANITY_MIN_EXP)
    dropped = sorted(set(exp_total) - set(valid))
    if dropped:
        print(f"⚠️ 비정상/미집계 월 제외 ({len(dropped)}개): {dropped}")
    if not valid:
        print("ERROR: 정상 수집된 월이 없습니다 (API 쿼터 소진/LOCK 의심). "
              "total을 갱신하지 않고 중단합니다.", file=sys.stderr)
        sys.exit(1)

    # DB 갱신: 옛 garbage 행을 모두 지우고 이번에 정상 수집된 월만 기록
    con = sqlite3.connect(db_path)
    con.execute("""CREATE TABLE IF NOT EXISTS trade_data (
        data_type   TEXT NOT NULL,
        hs_code     TEXT NOT NULL DEFAULT '',
        sub_code    TEXT NOT NULL DEFAULT '',
        entity_code TEXT NOT NULL DEFAULT '',
        ym          TEXT NOT NULL,
        exp_usd     INTEGER DEFAULT 0,
        imp_usd     INTEGER DEFAULT 0,
        wgt         INTEGER DEFAULT 0,
        PRIMARY KEY (data_type, hs_code, sub_code, entity_code, ym))""")
    if is_full:
        con.execute("DELETE FROM trade_data WHERE data_type='total'")
    else:
        # 증분: 이번에 수집한 월만 교체(옛 월 보존)
        con.executemany("DELETE FROM trade_data WHERE data_type='total' AND ym=?",
                        [(ym,) for ym in valid])
    rows = [("total", "", "", "", ym, exp_total[ym], imp_total[ym], 0)
            for ym in valid]
    con.executemany(
        "INSERT OR REPLACE INTO trade_data "
        "(data_type, hs_code, sub_code, entity_code, ym, exp_usd, imp_usd, wgt) "
        "VALUES (?,?,?,?,?,?,?,?)",
        rows
    )
    con.commit()
    con.close()
    print(f"DB trade_data 'total' {len(rows)}개월 갱신")

    # JSON 갱신: 이번에 정상 수집된 월만 기록 (옛 garbage 누출 방지)
    with open(json_path, "r", encoding="utf-8") as f:
        d = json.load(f)
    old_total = d.get("total", {}) or {}
    if is_full or not old_total.get("exp"):
        new_exp = {ym: exp_total[ym] for ym in valid}
        new_imp = {ym: imp_total[ym] for ym in valid}
    else:
        # 증분: 기존 total에 이번 수집월만 덮어쓰기(옛 월 보존)
        new_exp = dict(old_total.get("exp", {})); new_exp.update({ym: exp_total[ym] for ym in valid})
        new_imp = dict(old_total.get("imp", {})); new_imp.update({ym: imp_total[ym] for ym in valid})
    d["total"] = {"exp": new_exp, "imp": new_imp}
    from datetime import datetime
    d["total_generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, separators=(",", ":"))
    print(f"trade_data_v2.json 'total' {len(valid)}개월"
          f"({valid[0]}~{valid[-1]}) 갱신 완료 ({os.path.getsize(json_path):,} bytes)")
    print("DONE")


if __name__ == "__main__":
    main()
