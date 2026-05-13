#!/usr/bin/env python3
"""
ranking_6d 상위 HS6에 시군구별 수출 데이터 부착 (증분 수집)

- trade_data_v2.json의 ranking_6d 에서 수출액(전체월 합산) 기준 상위 N개 HS6 선정
- 메인 품목(items의 키) 6자리 HS는 제외 (이미 items[*].regions 에 수집됨, 중복 회피)
- 각 HS6에 대해 누락된 월만 수집 (증분)
- 결과는 ranking_6d[hs].regions = {지역코드: {name, exp:{ym:USD}}} 로 누적 머지
"""
import os, sys, io, json, time
from datetime import datetime
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from customs_trade_v2 import (
    api_call_xml, parse_ym_from_priod, safe_int, REQUEST_DELAY,
    get_sido_codes,
)

API_KEY = os.environ.get("API_KEY", "")
TOP_N = int(os.environ.get("RANKING_REGIONS_TOP_N", "500"))
TARGET_MONTHS = 14


def last_n_months(n):
    """현재 시점 기준 최근 n개월의 ym 목록"""
    now = datetime.now()
    out = []
    y, m = now.year, now.month
    for _ in range(n):
        out.append(f"{y}{m:02d}")
        m -= 1
        if m < 1:
            m = 12
            y -= 1
    return sorted(out)


def make_ranges(months):
    """월 목록을 (start, end) 구간으로 변환 (최대 12개월)"""
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


def collected_months_for_hs(hs_entry):
    """ranking_6d[hs].regions 안에 이미 수집된 ym 집합 반환"""
    out = set()
    for rv in (hs_entry.get("regions") or {}).values():
        for ym in (rv.get("exp") or {}).keys():
            out.add(ym)
    return out


def pick_top_hs(ranking, excluded, n):
    """수출액(전체월 합산) 상위 n개 HS6 선정. excluded(메인품목 HS6)는 제외."""
    scored = []
    for hs, v in ranking.items():
        if hs in excluded:
            continue
        if not hs or len(hs) != 6 or not hs.isdigit():
            continue
        total = sum((v.get("exp") or {}).values())
        if total > 0:
            scored.append((hs, total))
    scored.sort(key=lambda x: -x[1])
    return [hs for hs, _ in scored[:n]]


def collect_sigungu_one(hs6, sido_codes, date_ranges, api_key):
    """customs_trade_v2.collect_sigungu와 동일 로직 (단일 HS6) — 의존성 명시 위해 인라인"""
    sgg_exp = defaultdict(lambda: defaultdict(int))
    consecutive_locks = 0
    for sido in sido_codes:
        for start, end in date_ranges:
            try:
                rows = api_call_xml(
                    "/sigunguperprlstperacrs/getSigunguPerPrlstPerAcrs",
                    {"strtYymm": start, "endYymm": end, "HsSgn": hs6, "sidoCd": sido},
                    api_key,
                )
                consecutive_locks = 0
            except Exception as e:
                msg = str(e)
                if "LOCK" in msg.upper() or "LIMITED" in msg.upper():
                    consecutive_locks += 1
                    if consecutive_locks >= 3:
                        print(f"    [LOCK x{consecutive_locks}] 60s long pause", flush=True)
                        time.sleep(60)
                        consecutive_locks = 0
                rows = []
            for r in rows:
                ym = parse_ym_from_priod(r.get("priodTitle", ""))
                if not ym:
                    continue
                sgg_nm = (r.get("sggNm") or "").strip()
                exp = safe_int(r.get("expUsdAmt", 0)) * 1000  # 천USD → USD
                if sgg_nm and exp > 0:
                    sgg_exp[sgg_nm][ym] += exp
            time.sleep(REQUEST_DELAY)

    regions = {}
    for sgg_nm, months in sgg_exp.items():
        if not sgg_nm:
            continue
        regions[sgg_nm] = {"name": sgg_nm, "exp": dict(months)}
    return regions


def main():
    if not API_KEY:
        print("ERROR: API_KEY 환경변수 필요", file=sys.stderr)
        sys.exit(1)

    base = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base, "trade_data_v2.json")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    ranking = data.get("ranking_6d", {})
    items = data.get("items", {})
    excluded = set(items.keys())  # 메인 품목 HS는 items.regions에 이미 있음
    print(f"ranking_6d 총 {len(ranking)}개 HS6, 메인 품목 {len(excluded)}개 제외")

    targets = pick_top_hs(ranking, excluded, TOP_N)
    print(f"수출액 상위 {len(targets)}개 HS6 선정")

    target_months = set(last_n_months(TARGET_MONTHS))
    sido_codes = get_sido_codes()
    print(f"시도 {len(sido_codes)}개, 대상 월 {len(target_months)}개\n")

    total = len(targets)
    start_time = time.time()
    new_hs = 0
    updated_hs = 0

    for idx, hs in enumerate(targets, 1):
        entry = ranking.get(hs, {"name": "", "exp": {}, "wgt": {}, "countries": {}})
        collected = collected_months_for_hs(entry)
        missing = sorted(target_months - collected)
        if not missing:
            if idx % 50 == 0 or idx == total:
                elapsed = time.time() - start_time
                print(f"  [{idx}/{total}] {hs} skip (모든 월 수집됨) — {elapsed:.0f}s", flush=True)
            continue

        date_ranges = make_ranges(missing)
        regions = collect_sigungu_one(hs, sido_codes, date_ranges, API_KEY)

        if not regions:
            continue

        existing = entry.setdefault("regions", {})
        for rcode, rv in regions.items():
            if rcode in existing:
                exp_map = existing[rcode].setdefault("exp", {})
                for ym, val in (rv.get("exp") or {}).items():
                    exp_map[ym] = exp_map.get(ym, 0) + val
            else:
                existing[rcode] = rv
        ranking[hs] = entry

        was_new = len(collected) == 0
        if was_new:
            new_hs += 1
        else:
            updated_hs += 1

        if idx % 20 == 0 or idx == total:
            elapsed = time.time() - start_time
            pct = idx / total * 100
            eta = elapsed / idx * (total - idx)
            print(f"  [{idx}/{total}] {pct:.0f}% — {elapsed:.0f}s 경과, 잔여 {eta:.0f}s — 신규 {new_hs}, 갱신 {updated_hs}", flush=True)

    data["ranking_6d"] = ranking
    data["ranking_regions_generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    elapsed = time.time() - start_time
    print(f"\nDONE — {elapsed:.0f}초, 신규 {new_hs}개 HS, 갱신 {updated_hs}개 HS")


if __name__ == "__main__":
    main()
