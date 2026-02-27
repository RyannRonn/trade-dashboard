#!/usr/bin/env python3
"""보톡스/필러(BTX) 세부항목 수집: 10자리 HS코드 — 국가별 + 중량 포함"""
import os, sys, json, time
from collections import defaultdict

# customs_trade_v2.py에서 공통 함수 임포트
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from customs_trade_v2 import (
    api_call_xml, parse_ym_from_year, safe_int, get_date_ranges,
    REQUEST_DELAY, COUNTRY_NAMES
)

API_KEY = os.environ.get("API_KEY", "")

# 보톡스/필러 세부항목 (전체 합산 대상)
BTX_SUBS = {
    "3002491000": "보톡스",
    "3304999000": "필러",
}

# 수집 대상 국가
WANT_COUNTRIES = [
    "US","CN","JP","DE","VN","IN","BR","RU","TH","MY","AU","TR","ID","GB",
    "SA","AE","MX","IT","NL","FR","KW","SG","HK","TW","CA","PL","ES",
]

# 기업별 탭 — sigungu API로 HS 6자리 수집
# 휴젤/메디톡스: 보톡스(300249)+필러(330499) 별도 location
# 대웅제약: 보톡스(300249)만
BTX_COMPANIES = {
    "pharmaresearch": {
        "name": "파마리서치",
        "sidoCd": "51",
        "sggNm": "강원특별자치도 강릉시",
        "short_sgg": "강원 강릉시",
        "tracks": [
            {"key": "pharma_med",  "name": "의료기기 (강원 강릉시)", "hs6": "901890"},
            {"key": "pharma_cosm", "name": "화장품 (강원 강릉시)",   "hs6": "330499"},
        ],
    },
    "hugel": {
        "name": "휴젤",
        "sidoCd": "51",
        "sggNm": "강원특별자치도 춘천시",
        "short_sgg": "강원 춘천시",
        "tracks": [
            {"key": "hugel_btx",    "name": "보톡스 (강원 춘천시)", "hs6": "300249"},
            {"key": "hugel_filler", "name": "필러 (강원 춘천시)",   "hs6": "330499"},
        ],
    },
    "medytox": {
        "name": "메디톡스",
        "sidoCd": "43",
        "sggNm": "충청북도 청주시",
        "short_sgg": "충북 청주시",
        "tracks": [
            {"key": "medytox_btx",    "name": "보톡스 (충북 청주시)", "hs6": "300249"},
            {"key": "medytox_filler", "name": "필러 (충북 청주시)",   "hs6": "330499"},
        ],
    },
    "daewoong": {
        "name": "대웅제약",
        "sidoCd": "41",
        "sggNm": "경기도 화성시",
        "short_sgg": "경기 화성시",
        "tracks": [
            {"key": "daewoong_btx", "name": "보톡스 (경기 화성시)", "hs6": "300249"},
        ],
    },
}


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


def collect_single_hs6(hs6, sido, target_sgg, api_key, date_ranges):
    """단일 HS6 코드로 시군구 수출 데이터 수집"""
    from customs_trade_v2 import parse_ym_from_priod

    monthly = {}
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

    # BTX 항목이 없으면 기본 구조 생성
    if "BTX" not in data["items"]:
        data["items"]["BTX"] = {
            "name": "보톡스/필러",
            "total_exp": {},
            "total_imp": {},
            "total_wgt": {},
            "countries": {},
            "regions": {},
            "sub_items": {},
        }

    # 세부항목 수집
    sub_items = {}
    for hs_code, name in BTX_SUBS.items():
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
    data["items"]["BTX"]["total_exp"] = total_exp
    data["items"]["BTX"]["total_wgt"] = total_wgt

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
    data["items"]["BTX"]["countries"] = merged_countries
    print(f"\n전체 countries 재구축: {len(merged_countries)}개국")

    # sub_items 저장
    data["items"]["BTX"]["sub_items"] = sub_items

    # 기업별 시군구 수집 (보톡스/필러 별도 track)
    companies = {}
    for ckey, cinfo in BTX_COMPANIES.items():
        print(f"\n기업 '{cinfo['name']}' 시군구 수집 ({cinfo['sggNm']})...")
        locations = {}
        for track in cinfo["tracks"]:
            print(f"  {track['name']} (HS {track['hs6']})...")
            exp = collect_single_hs6(track["hs6"], cinfo["sidoCd"], cinfo["sggNm"], API_KEY, date_ranges)
            locations[track["key"]] = {"name": track["name"], "exp": exp}
            total = sum(exp.values())
            print(f"    -> {len(exp)}개월, 합계 {total:,} USD")
        companies[ckey] = {
            "name": cinfo["name"],
            "locations": locations,
        }

    data["items"]["BTX"]["companies"] = companies

    # main_items에 BTX 추가 (없으면)
    if "BTX" not in data.get("main_items", []):
        data.setdefault("main_items", []).append("BTX")

    # 3002, 2106 제거
    for rm_hs in ["3002", "2106"]:
        if rm_hs in data.get("main_items", []):
            data["main_items"].remove(rm_hs)

    # JSON 저장
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"\ntrade_data_v2.json 업데이트 완료 ({os.path.getsize(json_path):,} bytes)")
    print("DONE")


if __name__ == "__main__":
    main()
