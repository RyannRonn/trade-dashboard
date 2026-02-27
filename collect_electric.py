#!/usr/bin/env python3
"""전력(ELK) 세부항목 수집: 6자리 HS코드 — 국가별 + 중량 포함"""
import os, sys, json, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from customs_trade_v2 import (
    api_call_xml, parse_ym_from_year, safe_int, get_date_ranges,
    REQUEST_DELAY, COUNTRY_NAMES
)

API_KEY = os.environ.get("API_KEY", "")

# 전력 세부항목
ELK_SUBS = {
    "854460": "전선",
    "850423": "대형변압기",
    "850422": "중형변압기",
    "850421": "소형변압기",
    "853620": "차단기",
}

# 수집 대상 국가
WANT_COUNTRIES = [
    "US","CN","JP","DE","VN","IN","BR","RU","TH","MY","AU","TR","ID","GB",
    "SA","AE","MX","IT","NL","FR","SG","HK","TW","CA","PL","ES","PH","EG",
    "IQ","KW","QA","DZ","NG","BD","PK","KE",
]


def collect_sub_with_countries(hs_code, api_key, date_ranges):
    """nitemtrade API로 세부항목의 국가별 exp + wgt 수집"""
    country_exp = defaultdict(lambda: defaultdict(int))
    country_wgt = defaultdict(lambda: defaultdict(int))

    for start, end in date_ranges:
        print(f"    구간 {start}~{end} 수집...")
        rows = api_call_xml("/nitemtrade/getNitemtradeList",
                            {"strtYymm": start, "endYymm": end, "hsSgn": hs_code},
                            api_key)
        for r in rows:
            ym = parse_ym_from_year(r.get("year", ""))
            if not ym:
                continue
            stat_cd = r.get("statCd", "").strip()
            if not stat_cd or stat_cd == "-":
                continue
            country_exp[stat_cd][ym] += safe_int(r.get("expDlr", 0))
            country_wgt[stat_cd][ym] += safe_int(r.get("expWgt", 0))
        time.sleep(REQUEST_DELAY)

    # 총계 계산
    all_months = set()
    for cd in country_exp:
        all_months.update(country_exp[cd].keys())

    total_exp = {}
    total_wgt = {}
    for ym in all_months:
        total_exp[ym] = sum(country_exp[cd].get(ym, 0) for cd in country_exp)
        total_wgt[ym] = sum(country_wgt[cd].get(ym, 0) for cd in country_exp)

    # 국가 데이터 정리
    countries = {}
    for cd in WANT_COUNTRIES:
        if cd in country_exp and any(v > 0 for v in country_exp[cd].values()):
            countries[cd] = {
                "name": COUNTRY_NAMES.get(cd, cd),
                "exp": dict(country_exp[cd]),
                "wgt": dict(country_wgt[cd]),
            }

    return total_exp, total_wgt, countries


def main():
    if not API_KEY:
        print("ERROR: API_KEY 환경변수 필요", file=sys.stderr)
        sys.exit(1)

    date_ranges = get_date_ranges(14)
    print(f"수집 기간: {date_ranges}")

    # 기존 JSON 로드
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trade_data_v2.json")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # ELK 항목 기본 구조
    if "ELK" not in data["items"]:
        data["items"]["ELK"] = {
            "name": "전력",
            "total_exp": {},
            "total_imp": {},
            "total_wgt": {},
            "countries": {},
            "regions": {},
            "sub_items": {},
        }

    # 세부항목 수집
    sub_items = {}
    for hs_code, name in ELK_SUBS.items():
        print(f"\n{name} ({hs_code}) 수집...")
        total_exp, total_wgt, countries = collect_sub_with_countries(hs_code, API_KEY, date_ranges)
        sub_items[hs_code] = {
            "name": name,
            "exp": total_exp,
            "wgt": total_wgt,
            "countries": countries,
        }
        print(f"  -> {len(total_exp)}개월, 국가 {len(countries)}개")

    # 세부항목 합산 → total
    all_months = set()
    for si in sub_items.values():
        all_months.update(si["exp"].keys())

    total_exp = {}
    total_wgt = {}
    for ym in all_months:
        total_exp[ym] = sum(si["exp"].get(ym, 0) for si in sub_items.values())
        total_wgt[ym] = sum(si["wgt"].get(ym, 0) for si in sub_items.values())
    data["items"]["ELK"]["total_exp"] = total_exp
    data["items"]["ELK"]["total_wgt"] = total_wgt

    # countries 합산 재구축
    all_cds = set()
    for si in sub_items.values():
        all_cds.update(si.get("countries", {}).keys())
    merged_countries = {}
    for cd in all_cds:
        ce = defaultdict(int)
        cw = defaultdict(int)
        for si in sub_items.values():
            sc = si.get("countries", {}).get(cd, {})
            for ym, v in sc.get("exp", {}).items():
                ce[ym] += v
            for ym, v in sc.get("wgt", {}).items():
                cw[ym] += v
        merged_countries[cd] = {
            "name": COUNTRY_NAMES.get(cd, cd),
            "exp": dict(ce),
            "wgt": dict(cw),
        }
    data["items"]["ELK"]["countries"] = merged_countries
    print(f"\n전체 countries 재구축: {len(merged_countries)}개국")

    # sub_items 저장
    data["items"]["ELK"]["sub_items"] = sub_items

    # main_items에 ELK 추가
    if "ELK" not in data.get("main_items", []):
        data.setdefault("main_items", []).append("ELK")

    # JSON 저장
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"\ntrade_data_v2.json 업데이트 완료 ({os.path.getsize(json_path):,} bytes)")
    print("DONE")


if __name__ == "__main__":
    main()
