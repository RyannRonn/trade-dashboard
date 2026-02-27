#!/usr/bin/env python3
"""화장품(3304) 세부항목 수집: 330499(기초), 330410(색조) — 국가별 + 중량 포함"""
import os, sys, json, time
from collections import defaultdict

# customs_trade_v2.py에서 공통 함수 임포트
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from customs_trade_v2 import (
    api_call_xml, parse_ym_from_year, safe_int, get_date_ranges,
    REQUEST_DELAY, COUNTRY_NAMES
)

API_KEY = os.environ.get("API_KEY", "")

# 화장품 세부항목 (전체 합산 대상)
COSMETICS_SUBS = {
    "330499": "기초",
    "330410": "색조",
}

# 추가 세부항목 (별도 탭, 전체 합산에 미포함)
EXTRA_SUBS = {
    "8543702020": "홈뷰티디바이스",
}

# 수집 대상 국가 (화장품 주요국 + 권역 구성국)
WANT_COUNTRIES = [
    "US","CN","JP","VN","TH","RU","HK","MY","SG","AU","TW","ID","CA","IN",
    # 유럽
    "PL","GB","FR","NL","DE","ES","IT",
    # 중동
    "AE","QA","IL","SA","KW","TR",
    # 남미
    "BR","AR","MX","CL","CO","PE",
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


# 화장품 기업별 탭 — sigungu API로 6자리 HS 수집 (추정 없음)
COSMETICS_COMPANIES = {
    "siliconto": {
        "name": "실리콘투",
        "hs6_list": ["330499", "330410", "330420"],
        "sidoCd": "41",
        "sggNm": "경기도 성남시",
    },
    "inglewood": {
        "name": "잉글우드랩",
        "hs6_list": ["330499", "330410", "330420"],
        "sidoCd": "28",
        "sggNm": "인천광역시 남동구",
    },
}


def collect_company_sigungu(company, api_key, date_ranges):
    """기업 소재지 시군구의 화장품 수출 데이터 수집 (HS6 여러 개 합산)"""
    from customs_trade_v2 import parse_ym_from_priod

    hs6_list = company["hs6_list"]
    sido = company["sidoCd"]
    target_sgg = company["sggNm"]
    monthly = {}

    for hs6 in hs6_list:
        for start, end in date_ranges:
            rows = api_call_xml(
                "/sigunguperprlstperacrs/getSigunguPerPrlstPerAcrs",
                {"strtYymm": start, "endYymm": end, "HsSgn": hs6, "sidoCd": sido},
                api_key
            )
            for r in rows:
                ym = parse_ym_from_priod(r.get("priodTitle", ""))
                if not ym:
                    continue
                sgg_nm = r.get("sggNm", "").strip()
                if sgg_nm == target_sgg:
                    raw_exp = str(r.get("expUsdAmt", "0")).replace(",", "")
                    exp = safe_int(raw_exp) * 1000  # 천USD → USD 변환
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

    # 기존 JSON 로드
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trade_data_v2.json")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 3304 항목이 없으면 기본 구조 생성
    if "3304" not in data["items"]:
        print("3304 항목이 없으므로 nitemtrade로 전체 데이터 먼저 수집...")
        from customs_trade_v2 import collect_nitemtrade
        total_exp, total_imp, countries = collect_nitemtrade("3304", API_KEY, date_ranges, WANT_COUNTRIES)
        data["items"]["3304"] = {
            "name": "화장품",
            "total_exp": total_exp,
            "total_imp": total_imp,
            "total_wgt": {},
            "countries": countries,
            "regions": {},
            "sub_items": {},
        }
        print(f"  3304 전체: {len(total_exp)}개월, 국가 {len(countries)}개")

    # 세부항목 수집
    sub_items = {}
    for hs_code, name in COSMETICS_SUBS.items():
        print(f"\n{name} ({hs_code}) 수집...")
        total_exp, total_wgt, countries = collect_sub_with_countries(hs_code, API_KEY, date_ranges)
        sub_items[hs_code] = {
            "name": name,
            "exp": total_exp,
            "wgt": total_wgt,
            "countries": countries,
        }
        print(f"  -> {len(total_exp)}개월, 국가 {len(countries)}개")

    # 세부항목 합산으로 total_exp, total_wgt, countries 전체 재구축
    all_months = set()
    for si in sub_items.values():
        all_months.update(si["exp"].keys())

    total_exp = {}
    total_wgt = {}
    for ym in all_months:
        total_exp[ym] = sum(si["exp"].get(ym, 0) for si in sub_items.values())
        total_wgt[ym] = sum(si["wgt"].get(ym, 0) for si in sub_items.values())
    data["items"]["3304"]["total_exp"] = total_exp
    data["items"]["3304"]["total_wgt"] = total_wgt

    # countries를 세부항목에서 합산 재구축
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
    data["items"]["3304"]["countries"] = merged_countries
    print(f"\n전체 countries 재구축: {len(merged_countries)}개국")

    # 추가 세부항목 수집 (전체 합산에 미포함)
    for hs_code, name in EXTRA_SUBS.items():
        print(f"\n{name} ({hs_code}) 수집...")
        total_exp_e, total_wgt_e, countries_e = collect_sub_with_countries(hs_code, API_KEY, date_ranges)
        sub_items[hs_code] = {
            "name": name,
            "exp": total_exp_e,
            "wgt": total_wgt_e,
            "countries": countries_e,
        }
        print(f"  -> {len(total_exp_e)}개월, 국가 {len(countries_e)}개")

    # sub_items 저장
    data["items"]["3304"]["sub_items"] = sub_items

    # 기업별 시군구 수집 (추정 없이 실제 데이터)
    companies = {}
    for ckey, cinfo in COSMETICS_COMPANIES.items():
        print(f"\n기업 '{cinfo['name']}' 시군구 수집 (HS {cinfo['hs6_list']}, {cinfo['sggNm']})...")
        exp = collect_company_sigungu(cinfo, API_KEY, date_ranges)
        # 시군구명을 sggNm에서 축약 (경기도 성남시 → 경기 성남시)
        short_sgg = cinfo["sggNm"].replace("특별시 ", "").replace("광역시 ", "").replace("특별자치도 ", "").replace("도 ", " ").replace("  ", " ")
        companies[ckey] = {
            "name": cinfo["name"],
            "locations": {
                ckey: {"name": short_sgg, "exp": exp}
            }
        }
        print(f"  -> {len(exp)}개월, 합계 {sum(exp.values()):,} USD")
    data["items"]["3304"]["companies"] = companies

    # main_items에 3304 추가 (없으면)
    if "3304" not in data.get("main_items", []):
        data.setdefault("main_items", []).append("3304")

    # JSON 저장
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"\ntrade_data_v2.json 업데이트 완료 ({os.path.getsize(json_path):,} bytes)")
    print("DONE")


if __name__ == "__main__":
    main()
