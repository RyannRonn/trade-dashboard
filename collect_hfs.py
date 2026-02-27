#!/usr/bin/env python3
"""건기식(HFS) 수집: HS 210690 — 국가별 + 중량 + 기업별 시군구"""
import os, sys, json, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from customs_trade_v2 import (
    api_call_xml, parse_ym_from_year, parse_ym_from_priod,
    safe_int, get_date_ranges, REQUEST_DELAY, COUNTRY_NAMES
)

API_KEY = os.environ.get("API_KEY", "")

HS_CODE = "210690"

WANT_COUNTRIES = [
    "US","CN","JP","DE","VN","IN","BR","RU","TH","MY","AU","TR","ID","GB",
    "SA","AE","MX","IT","NL","FR","SG","HK","TW","CA","PL","ES","PH",
]

HFS_COMPANIES = {
    "novarex": {
        "name": "노바렉스",
        "hs6": "210690",
        "sidoCd": "43",
        "sggNm": "충청북도 청주시",
    },
    "cosmaxnbt": {
        "name": "코스맥스엔비티",
        "hs6": "210690",
        "sidoCd": "41",
        "sggNm": "경기도 성남시",
    },
}


def collect_with_countries(hs_code, api_key, date_ranges):
    """nitemtrade API로 국가별 exp + wgt 수집"""
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

    all_months = set()
    for cd in country_exp:
        all_months.update(country_exp[cd].keys())

    total_exp = {}
    total_wgt = {}
    for ym in all_months:
        total_exp[ym] = sum(country_exp[cd].get(ym, 0) for cd in country_exp)
        total_wgt[ym] = sum(country_wgt[cd].get(ym, 0) for cd in country_exp)

    countries = {}
    for cd in WANT_COUNTRIES:
        if cd in country_exp and any(v > 0 for v in country_exp[cd].values()):
            countries[cd] = {
                "name": COUNTRY_NAMES.get(cd, cd),
                "exp": dict(country_exp[cd]),
                "wgt": dict(country_wgt[cd]),
            }

    return total_exp, total_wgt, countries


def collect_company_sigungu(company, api_key, date_ranges):
    """기업 소재지 시군구 수출 데이터 수집"""
    monthly = {}
    for start, end in date_ranges:
        rows = api_call_xml(
            "/sigunguperprlstperacrs/getSigunguPerPrlstPerAcrs",
            {"strtYymm": start, "endYymm": end, "HsSgn": company["hs6"], "sidoCd": company["sidoCd"]},
            api_key
        )
        for r in rows:
            ym = parse_ym_from_priod(r.get("priodTitle", ""))
            if not ym:
                continue
            sgg_nm = r.get("sggNm", "").strip()
            if sgg_nm == company["sggNm"]:
                raw_exp = str(r.get("expUsdAmt", "0")).replace(",", "")
                exp = safe_int(raw_exp) * 1000
                if exp > 0:
                    monthly[ym] = monthly.get(ym, 0) + exp
        time.sleep(REQUEST_DELAY)
    return monthly


def main():
    if not API_KEY:
        print("ERROR: API_KEY 환경변수 필요", file=sys.stderr)
        sys.exit(1)

    date_ranges = get_date_ranges(14)
    print(f"수집 기간: {date_ranges}")

    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trade_data_v2.json")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 210690 수집
    print(f"\n건기식 ({HS_CODE}) 수집...")
    total_exp, total_wgt, countries = collect_with_countries(HS_CODE, API_KEY, date_ranges)
    print(f"  -> {len(total_exp)}개월, 국가 {len(countries)}개")

    # sub_items에 210690 저장 (rItemSubs 진입 조건)
    sub_items = {
        HS_CODE: {
            "name": "건기식",
            "exp": total_exp,
            "wgt": total_wgt,
            "countries": countries,
        }
    }

    data["items"]["HFS"] = {
        "name": "건기식",
        "total_exp": total_exp,
        "total_imp": {},
        "total_wgt": total_wgt,
        "countries": countries,
        "regions": {},
        "sub_items": sub_items,
    }

    # 기업별 시군구 수집
    companies = {}
    for ckey, cinfo in HFS_COMPANIES.items():
        print(f"\n기업 '{cinfo['name']}' 시군구 수집 (HS {cinfo['hs6']}, {cinfo['sggNm']})...")
        exp = collect_company_sigungu(cinfo, API_KEY, date_ranges)
        short_sgg = cinfo["sggNm"].replace("특별시 ", "").replace("광역시 ", "").replace("특별자치도 ", "").replace("도 ", " ").replace("  ", " ")
        companies[ckey] = {
            "name": cinfo["name"],
            "locations": {
                ckey: {"name": short_sgg, "exp": exp}
            }
        }
        print(f"  -> {len(exp)}개월, 합계 {sum(exp.values()):,} USD")

    data["items"]["HFS"]["companies"] = companies

    # main_items에 HFS 추가
    if "HFS" not in data.get("main_items", []):
        data.setdefault("main_items", []).append("HFS")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"\ntrade_data_v2.json 업데이트 완료 ({os.path.getsize(json_path):,} bytes)")
    print("DONE")


if __name__ == "__main__":
    main()
