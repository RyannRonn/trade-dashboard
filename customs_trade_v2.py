#!/usr/bin/env python3
"""
관세청 OpenAPI 수출입 데이터 수집 스크립트 v2
- 환경변수 API_KEY로 인증
- 2개 API 엔드포인트 사용:
  1) /nitemtrade/getNitemtradeList — 품목별 국가별 수출입 (품목 총계 + 국가별 동시 수집)
  2) /sigunguperprlstperacrs/getSigunguPerPrlstPerAcrs — 시군구별 품목별 수출입
- trade.html의 DEMO 데이터를 실제 데이터로 교체
- trade_data_v2.json 별도 저장
"""

import os
import sys
import json
import time
import re
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import xml.etree.ElementTree as ET
from collections import defaultdict

# ===== 설정 =====
API_BASE = "https://apis.data.go.kr/1220000"
MAX_RETRIES = 3
RETRY_DELAY = 2
REQUEST_DELAY = 0.3

# 품목 설정
ITEMS = {
    "8541": {"name": "반도체소자", "countries": ["US","CN","JP","TW","VN","DE","HK","SG","MY","IN","NL","PH","TH","IE","HU","PL","MX","GB"]},
    "8542": {"name": "반도체", "countries": ["US","CN","JP","TW","VN","DE","HK","SG","MY","IN","NL","PH","TH","IE","HU","PL","MX","GB"]},
    "8703": {"name": "승용차", "countries": ["US","DE","AU","SA","CA"]},
    "8507": {"name": "2차전지", "countries": ["US","DE","HU","PL","CN"]},
    "3304": {"name": "화장품", "countries": ["US","CN","JP","VN","TH","RU","HK","MY","SG","AU","TW","ID","CA"]},
    "1902301010": {"name": "라면", "countries": ["CN","US","JP","VN","PH","TH","AU","MY","ID","CA","GB","RU","DE","HK","SG","TW","NL","AE"], "sigungu_hs6": ["190230"], "months": 24},
    "9018": {"name": "미용의료기기", "countries": ["US","CN","JP","DE","VN","IN","BR","RU","TH","MY","AU","TR","ID","GB","SA","AE","MX","IT","NL","FR"]},
    "2710": {"name": "석유제품", "countries": ["CN","JP","SG","AU","IN"]},
    "7208": {"name": "열연강판", "countries": ["CN","VN","IN","JP","TH"]},
    "8901": {"name": "선박", "countries": ["SG","PA","MH","LR","GR"]},
    "BTX": {"name": "보톡스/필러", "countries": ["US","CN","JP","DE","VN","IN","BR","RU","TH","MY","AU","TR","ID","GB","SA","AE","MX","IT","NL","FR","SG","HK","TW","CA","PL","ES"]},
    "8517": {"name": "통신기기", "countries": ["US","CN","VN","IN","JP"]},
    "8486": {"name": "반도체장비", "countries": ["CN","TW","US","JP","SG"]},
    "8471": {"name": "컴퓨터", "countries": ["US","CN","JP","VN","DE"]},
    "ELK": {"name": "전력", "countries": ["US","CN","JP","DE","VN","IN","BR","RU","TH","MY","AU","TR","ID","GB","SA","AE","MX","IT","NL","FR","SG","PH","EG","IQ","KW","QA"]},
    "HFS": {"name": "건기식", "countries": ["US","CN","JP","VN","TH","AU","MY","HK","SG","TW","PH","ID","CA","RU","DE","AE","GB"]},
}

MAIN_ITEMS = ["8542", "ELK", "1902301010", "3304", "9018", "BTX", "HFS"]

SUB_ITEMS = {
    "8542": {
        "8542321010": "디램",
        "8542321030": "낸드",
        "8542323000": "복합구조칩",
        "7410211000": "CCL",
        "8532240000": "MLCC",
        "8534002000": "기판",
        "3701991000": "블랭크마스크",
        "8536691000": "테스트소켓",
        "3707901010": "감광액",
    },
}

# 라면(1902) 삼양 기업 설정 — sigungu API로 HS 190230(6자리) 수집
SAMYANG_CFG = {
    "name": "삼양식품",
    "hs6": "190230",
    "locations": {
        "seongbuk":  {"name": "서울 성북구",  "sidoCd": "11", "sggNm": "서울특별시 성북구"},
        "wonju":     {"name": "강원 원주시",  "sidoCd": "51", "sggNm": "강원특별자치도 원주시"},
        "iksan":     {"name": "전북 익산시",  "sidoCd": "52", "sggNm": "전북특별자치도 익산시"},
        "miryang":   {"name": "경남 밀양시",  "sidoCd": "48", "sggNm": "경상남도 밀양시"},
    }
}

# 시군구 코드 → 시도 코드(2자리) 매핑
REGIONS = [
    "4145","4113","4139","4131","4111","4115","4121","4155","4143",
    "2817","2826","2811","2871",
    "1120","1121","1114","1111","1123",
    "2611","2644","2617",
    "3114","3111",
    "4411","4413","4311","4717","4811",
    "2711","3014","4619","5011","1118"
]

# 시군구 API 응답의 sggNm → 코드 매핑용
REGION_NAMES = {
    "4145":"경기 화성시","4113":"경기 성남시","4139":"경기 이천시","4131":"경기 평택시",
    "4111":"경기 수원시","4115":"경기 용인시","4121":"경기 안산시","4155":"경기 시흥시",
    "4143":"경기 파주시","2817":"인천 남동구","2826":"인천 서구","2811":"인천 중구",
    "2871":"인천 연수구","1120":"서울 강남구","1121":"서울 송파구","1114":"서울 중구",
    "1111":"서울 종로구","1123":"서울 서초구","2611":"부산 중구","2644":"부산 강서구",
    "2617":"부산 사하구","3114":"울산 남구","3111":"울산 중구","4411":"충남 천안시",
    "4413":"충남 아산시","4311":"충북 청주시","4717":"경북 구미시","4811":"경남 창원시",
    "2711":"대구 중구","3014":"대전 유성구","4619":"전남 광양시","5011":"제주 제주시",
    "3611":"세종 세종시","1118":"서울 성동구"
}

COUNTRY_NAMES = {
    "US":"미국","CN":"중국","JP":"일본","DE":"독일","VN":"베트남","TW":"대만",
    "IN":"인도","SG":"싱가포르","AU":"호주","SA":"사우디","TH":"태국",
    "MY":"말레이시아","HK":"홍콩","GB":"영국","NL":"네덜란드","HU":"헝가리",
    "PL":"폴란드","CA":"캐나다","FR":"프랑스","PH":"필리핀","ID":"인도네시아",
    "RU":"러시아","MX":"멕시코","BR":"브라질","AE":"UAE","TR":"튀르키예",
    "IT":"이탈리아","ES":"스페인","CZ":"체코","IE":"아일랜드",
    "PA":"파나마","MH":"마셜제도","LR":"라이베리아","GR":"그리스",
    "QA":"카타르","IL":"이스라엘","KW":"쿠웨이트",
    "AR":"아르헨티나","CL":"칠레","CO":"콜롬비아","PE":"페루"
}

# sggNm(API 응답) → 코드 역매핑 (도이름 시군구명 → 코드)
SGG_NAME_TO_CODE = {}
for code, name in REGION_NAMES.items():
    # "경기 화성시" → "경기도 화성시" 등 변환 패턴
    parts = name.split(" ")
    if len(parts) == 2:
        SGG_NAME_TO_CODE[name] = code
        # API가 "경기도 화성시" 형태로 반환하므로 다양한 매핑
        sido_map = {
            "경기": "경기도", "인천": "인천광역시", "서울": "서울특별시",
            "부산": "부산광역시", "울산": "울산광역시", "충남": "충청남도",
            "충북": "충청북도", "경북": "경상북도", "경남": "경상남도",
            "대구": "대구광역시", "대전": "대전광역시", "전남": "전라남도",
            "제주": "제주특별자치도", "세종": "세종특별자치시"
        }
        if parts[0] in sido_map:
            SGG_NAME_TO_CODE[f"{sido_map[parts[0]]} {parts[1]}"] = code
# 세종은 시군구 없이 "세종특별자치시"로 반환
SGG_NAME_TO_CODE["세종특별자치시"] = "3611"


def get_date_ranges(months=14):
    """최근 N개월을 1년 이내 구간으로 분할 (API 제한: 조회기간 1년 이내)"""
    now = datetime.now()
    ranges = []
    end_y, end_m = now.year, now.month

    remaining = months
    while remaining > 0:
        chunk = min(remaining, 12)
        # end
        end_ym = f"{end_y}{end_m:02d}"
        # start
        s_m = end_m - chunk + 1
        s_y = end_y
        while s_m < 1:
            s_m += 12
            s_y -= 1
        start_ym = f"{s_y}{s_m:02d}"
        ranges.append((start_ym, end_ym))
        # next chunk ends before this start
        end_m = s_m - 1
        end_y = s_y
        if end_m < 1:
            end_m += 12
            end_y -= 1
        remaining -= chunk

    return ranges


RECENT_MONTHS = int(os.environ.get("RECENT_MONTHS", "3"))
FULL_REBUILD = os.environ.get("FULL_REBUILD") == "1"


def _ym_add(ym, delta):
    """'YYYYMM'에 delta개월 더한 'YYYYMM'."""
    y, m = int(ym[:4]), int(ym[4:])
    idx = y * 12 + (m - 1) + delta
    return f"{idx // 12}{idx % 12 + 1:02d}"


def _window_ranges(start_ym, end_ym):
    """start~end(YYYYMM 포함) 구간을 12개월 이하 청크로 분할. 최신 청크가 [0]."""
    months = []
    cur = start_ym
    while cur <= end_ym:
        months.append(cur)
        cur = _ym_add(cur, 1)
    ranges = []
    i = len(months)
    while i > 0:
        chunk = months[max(0, i - 12):i]
        ranges.append((chunk[0], chunk[-1]))
        i -= 12
    return ranges


def existing_months_in_json(json_path):
    """기존 trade_data_v2.json에 이미 들어있는 월(YYYYMM) 집합."""
    if not os.path.exists(json_path):
        return set()
    try:
        old = json.load(open(json_path, encoding="utf-8"))
    except Exception:
        return set()
    yms = set((old.get("total", {}) or {}).get("exp", {}).keys())
    if not yms:
        for it in (old.get("items", {}) or {}).values():
            yms.update((it.get("total_exp") or {}).keys())
    return yms


def get_incremental_ranges(json_path, recent=None, full=14):
    """기존 JSON 있으면 최근 recent개월(공백 있으면 보충), 없거나 FULL_REBUILD면 full개월 전체.
    반환: (ranges, is_full). 머지는 merge_with_existing가 옛 달을 보존한다."""
    recent = recent or RECENT_MONTHS
    existing = set() if FULL_REBUILD else existing_months_in_json(json_path)
    if not existing:
        return get_date_ranges(full), True
    now = datetime.now()
    end = f"{now.year}{now.month:02d}"
    start = _ym_add(end, -(recent - 1))
    latest = max(existing)
    # 마지막 수집월과 윈도우 사이에 공백이 생기면 start를 당겨 메운다
    if _ym_add(latest, 1) < start:
        start = _ym_add(latest, -(recent - 1))
    if start > end:
        start = end
    return _window_ranges(start, end), False


def api_call_xml(path, params, api_key):
    """관세청 API 호출 (XML 기본, 재시도 포함)"""
    query_params = {
        "serviceKey": api_key,
        "numOfRows": "10000",
        **params
    }
    url = f"{API_BASE}{path}?{urlencode(query_params)}"

    for attempt in range(MAX_RETRIES):
        try:
            req = Request(url)
            with urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8")

            # XML 파싱
            try:
                root = ET.fromstring(raw)
                # 에러 체크
                result_code = root.findtext(".//resultCode")
                if result_code and result_code != "00":
                    msg = root.findtext(".//resultMsg", "")
                    print(f"  [API] code={result_code} msg={msg}", file=sys.stderr)
                    if "SERVICE_KEY" in msg:
                        return []

                items = []
                for item in root.findall(".//item"):
                    row = {}
                    for child in item:
                        row[child.tag] = (child.text or "").strip()
                    items.append(row)
                return items
            except ET.ParseError:
                pass

            # JSON 폴백
            try:
                data = json.loads(raw)
                body = data.get("response", {}).get("body", {})
                items = body.get("items", {})
                if isinstance(items, dict):
                    items = items.get("item", [])
                if isinstance(items, dict):
                    items = [items]
                return items if isinstance(items, list) else []
            except (json.JSONDecodeError, AttributeError):
                pass

            print(f"  [WARN] 파싱 실패 (attempt {attempt+1}): {raw[:200]}", file=sys.stderr)

        except HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            print(f"  [WARN] HTTP {e.code} (attempt {attempt+1}): {body}", file=sys.stderr)
            if e.code == 403:
                return []
        except (URLError, TimeoutError, OSError) as e:
            print(f"  [WARN] 요청 실패 (attempt {attempt+1}): {e}", file=sys.stderr)

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY * (attempt + 1))

    return []


def parse_ym_from_year(year_str):
    """'2025.01' or '총계' → 'YYYYMM' or None"""
    if not year_str or year_str == "총계":
        return None
    year_str = year_str.strip()
    # "2025.01" → "202501"
    if "." in year_str:
        parts = year_str.split(".")
        if len(parts) == 2:
            return f"{parts[0]}{int(parts[1]):02d}"
    # "202501" 이미 YYYYMM 형태
    if len(year_str) == 6 and year_str.isdigit():
        return year_str
    return None


def parse_ym_from_priod(priod):
    """priodTitle '2025.01' → 'YYYYMM'"""
    return parse_ym_from_year(priod)


def safe_int(v):
    """문자열 → 정수 (공백, 천단위 콤마, None 처리)

    sigungu API(getSigunguPerPrlstPerAcrs)의 expUsdAmt는 값이 1,000 이상이면
    천단위 콤마를 넣어 반환한다("48,734"). 콤마를 제거하지 않으면 float()가
    ValueError를 던져 큰 시군구 수출이 통째로 0 처리·폐기된다.
    (nitemtrade의 expDlr 등은 콤마 없는 형식이라 replace는 no-op)
    """
    try:
        return round(float(str(v or "0").strip().replace(",", "") or "0"))
    except (ValueError, TypeError):
        return 0


def collect_nitemtrade(hs, api_key, date_ranges, want_countries):
    """
    /nitemtrade/getNitemtradeList 호출로 품목의 총계 + 국가별 데이터 동시 수집
    한 번 호출하면 해당 HS의 모든 국가 × 6자리코드별 데이터가 반환됨
    → 총계는 합산, 국가별은 want_countries에 해당하는 것만 추출
    """
    total_exp = {}
    total_imp = {}
    country_exp = defaultdict(lambda: defaultdict(int))
    country_imp = defaultdict(lambda: defaultdict(int))

    for start, end in date_ranges:
        rows = api_call_xml("/nitemtrade/getNitemtradeList",
                            {"strtYymm": start, "endYymm": end, "hsSgn": hs},
                            api_key)
        for r in rows:
            yr = r.get("year", "")
            ym = parse_ym_from_year(yr)
            if not ym:
                continue
            stat_cd = r.get("statCd", "").strip()
            exp = safe_int(r.get("expDlr", 0))
            imp = safe_int(r.get("impDlr", 0))

            if stat_cd and stat_cd != "-":
                # 국가별 데이터 (6자리 코드별이므로 국가로 합산)
                country_exp[stat_cd][ym] += exp
                country_imp[stat_cd][ym] += imp
            # 총계는 국가별 합산으로 계산 (총계 행은 기간 전체 합산이라 월별 아님)

        time.sleep(REQUEST_DELAY)

    # 국가별 합산에서 총계 계산
    all_months = set()
    for cd in country_exp:
        all_months.update(country_exp[cd].keys())
    for ym in all_months:
        total_exp[ym] = sum(country_exp[cd].get(ym, 0) for cd in country_exp)
        total_imp[ym] = sum(country_imp[cd].get(ym, 0) for cd in country_imp)

    # want_countries에 해당하는 국가만 추출
    countries = {}
    for cd in want_countries:
        if cd in country_exp and any(v > 0 for v in country_exp[cd].values()):
            countries[cd] = {
                "name": COUNTRY_NAMES.get(cd, cd),
                "exp": dict(country_exp[cd])
            }

    return total_exp, total_imp, countries


def get_sido_codes():
    """시군구 수집 대상 시도 코드 (전국 17개 시도)"""
    return [
        "11", "26", "27", "28", "29", "30", "31", "36",
        "41", "43", "44", "46", "47", "48", "50",
        "51", "52",
    ]


def collect_sigungu(hs, hs6_codes, api_key, date_ranges):
    """
    /sigunguperprlstperacrs/getSigunguPerPrlstPerAcrs 로 시군구별 데이터 수집
    - 6자리 HS코드 필수 → 주요 하위코드만 사용 후 합산
    - sidoCd(시도 2자리) 필수
    """
    sido_codes = get_sido_codes()
    # 시군구명 → {ym: exp} 합산
    sgg_exp = defaultdict(lambda: defaultdict(int))

    for hs6 in hs6_codes:
        for sido in sido_codes:
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
                    exp = safe_int(r.get("expUsdAmt", 0)) * 1000  # 천USD → USD 변환
                    if sgg_nm and exp > 0:
                        sgg_exp[sgg_nm][ym] += exp

                time.sleep(REQUEST_DELAY)

    # 응답의 모든 시군구를 sggNm("경기도 화성시" 등)을 키 + 표시명으로 그대로 사용
    regions = {}
    for sgg_nm, months in sgg_exp.items():
        if not sgg_nm:
            continue
        regions[sgg_nm] = {"name": sgg_nm, "exp": dict(months)}

    return regions


def collect_samyang(api_key, date_ranges):
    """삼양식품 사업장별 수출 수집 (sigungu API, HS 190230)"""
    cfg = SAMYANG_CFG
    hs6 = cfg["hs6"]
    result = {}

    for loc_key, loc_info in cfg["locations"].items():
        sido = loc_info["sidoCd"]
        target_sgg = loc_info["sggNm"]
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
                    exp = safe_int(str(r.get("expUsdAmt", "0")).replace(",", "")) * 1000
                    if exp > 0:
                        monthly[ym] = monthly.get(ym, 0) + exp
            time.sleep(REQUEST_DELAY)

        result[loc_key] = {"name": loc_info["name"], "exp": monthly}
        print(f"    {loc_info['name']}: {len(monthly)}개월")

    return result


def collect_data(api_key):
    """전체 데이터 수집 (증분: 기존 JSON 있으면 최근 N개월만, 옛 달은 merge_with_existing가 보존)"""
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trade_data_v2.json")
    date_ranges, is_full = get_incremental_ranges(json_path)
    overall_start = date_ranges[-1][0] if date_ranges else ""
    overall_end = date_ranges[0][1] if date_ranges else ""
    mode = "전체 재수집(14개월)" if is_full else f"증분(최근 {RECENT_MONTHS}개월+공백)"
    print(f"수집 모드: {mode}")
    print(f"수집 기간: {overall_start} ~ {overall_end}")
    print(f"구간 분할: {date_ranges}")

    result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "period": {"start": overall_start, "end": overall_end},
        "main_items": MAIN_ITEMS,
        "sub_items_def": SUB_ITEMS,
        "all_countries": COUNTRY_NAMES,
        "all_regions": REGION_NAMES,
        "total": {"exp": {}, "imp": {}},
        "items": {}
    }

    all_items = list(ITEMS.items())
    total_count = len(all_items)

    for idx, (hs, cfg) in enumerate(all_items):
        print(f"\n[{idx+1}/{total_count}] {cfg['name']} ({hs}) 수집 중...")

        # 품목별 수집 기간: cfg["months"] 지정 시 그 기간, 아니면 기본
        item_ranges = get_date_ranges(cfg["months"]) if (is_full and cfg.get("months")) else date_ranges

        # 1) nitemtrade: 품목 총계 + 국가별 동시 수집
        print(f"  품목+국가별 데이터 수집...")
        total_exp, total_imp, countries = collect_nitemtrade(
            hs, api_key, item_ranges, cfg.get("countries", [])
        )

        item = {
            "name": cfg["name"],
            "total_exp": total_exp,
            "total_imp": total_imp,
            "countries": countries,
            "regions": {}
        }

        # 총계에 합산
        for ym in total_exp:
            result["total"]["exp"][ym] = result["total"]["exp"].get(ym, 0) + total_exp[ym]
        for ym in total_imp:
            result["total"]["imp"][ym] = result["total"]["imp"].get(ym, 0) + total_imp[ym]

        # 2) 시군구별: cfg["sigungu_hs6"] 지정 시 그 6자리 코드 사용(10자리 품목 대응),
        #    아니면 nitemtrade 응답에서 수출액 상위 3개 6자리 자동 추출
        if cfg.get("sigungu_hs6"):
            hs6_codes = cfg["sigungu_hs6"]
        else:
            hs6_codes = get_top_hs6_codes(hs, api_key, item_ranges, top_n=3)
        if hs6_codes:
            print(f"  시군구별 수집 (HS6: {hs6_codes})...")
            item["regions"] = collect_sigungu(hs, hs6_codes, api_key, item_ranges)

        result["items"][hs] = item
        print(f"  -> {len(total_exp)}개월, 국가 {len(countries)}개, 시군구 {len(item['regions'])}개")

    # 3) 세부항목 (국가별 + 중량 포함)
    print(f"\n세부항목 수집...")
    for parent_hs, subs in SUB_ITEMS.items():
        if parent_hs not in result["items"]:
            continue
        parent_countries = result["items"][parent_hs].get("countries", {})
        want_cds = list(parent_countries.keys())
        result["items"][parent_hs]["sub_items"] = {}
        for scode, sname in subs.items():
            print(f"  {sname} ({scode})...")
            country_exp = defaultdict(lambda: defaultdict(int))
            country_wgt = defaultdict(lambda: defaultdict(int))
            for start, end in date_ranges:
                rows = api_call_xml("/nitemtrade/getNitemtradeList",
                                    {"strtYymm": start, "endYymm": end, "hsSgn": scode},
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
            # 국가 데이터
            countries = {}
            for cd in want_cds:
                if cd in country_exp and any(v > 0 for v in country_exp[cd].values()):
                    countries[cd] = {
                        "name": COUNTRY_NAMES.get(cd, cd),
                        "exp": dict(country_exp[cd]),
                        "wgt": dict(country_wgt[cd]),
                    }
            if total_exp:
                result["items"][parent_hs]["sub_items"][scode] = {
                    "name": sname, "exp": total_exp, "wgt": total_wgt, "countries": countries
                }

    # 4) 삼양 기업 데이터 (라면 1902301010에 추가)
    if "1902301010" in result["items"]:
        ramen_cfg = ITEMS.get("1902301010", {})
        samyang_ranges = get_date_ranges(ramen_cfg["months"]) if (is_full and ramen_cfg.get("months")) else date_ranges
        print(f"\n삼양식품 수집 (HS {SAMYANG_CFG['hs6']})...")
        samyang_locs = collect_samyang(api_key, samyang_ranges)
        result["items"]["1902301010"]["samyang"] = samyang_locs

    return result


def get_top_hs6_codes(hs, api_key, date_ranges, top_n=3):
    """nitemtrade에서 해당 HS4의 수출액 상위 N개 6자리 코드 추출 (가장 최근 구간만 사용)"""
    if not date_ranges:
        return []

    start, end = date_ranges[0]  # 가장 최근 구간
    rows = api_call_xml("/nitemtrade/getNitemtradeList",
                        {"strtYymm": start, "endYymm": end, "hsSgn": hs},
                        api_key)

    hs6_exp = defaultdict(int)
    for r in rows:
        ym = parse_ym_from_year(r.get("year", ""))
        if not ym:
            continue
        hc = r.get("hsCd", "").strip()
        if hc and hc != "-" and len(hc) == 6:
            hs6_exp[hc] += safe_int(r.get("expDlr", 0))

    # 상위 N개
    sorted_codes = sorted(hs6_exp.items(), key=lambda x: x[1], reverse=True)
    return [code for code, _ in sorted_codes[:top_n]]


def _merge_ym_dict(old, new):
    """{ym: val} 형태 dict 머지: 새 값으로 덮고 새에 없는 옛 키는 유지."""
    out = dict(old) if isinstance(old, dict) else {}
    if isinstance(new, dict):
        out.update(new)
    return out


def merge_with_existing(new_data, json_path):
    """새로 수집한 데이터를 기존 trade_data_v2.json과 머지.
    월별 시계열은 모두 누적 (같은 (HS·국가·월) 키만 새 값으로 덮음),
    메타/사전류·name 등은 새 값 우선. ranking_6d는 기존 그대로 유지
    (collect_ranking.py가 DB→JSON으로 재빌드)."""
    if not os.path.exists(json_path):
        return new_data
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            old = json.load(f)
    except Exception as e:
        print(f"[WARN] 기존 JSON 로드 실패, 덮어쓰기로 진행: {e}", file=sys.stderr)
        return new_data

    out = dict(new_data)

    # total
    et = old.get("total", {}) or {}
    nt = new_data.get("total", {}) or {}
    out["total"] = {
        "exp": _merge_ym_dict(et.get("exp", {}), nt.get("exp", {})),
        "imp": _merge_ym_dict(et.get("imp", {}), nt.get("imp", {})),
    }

    # items: hs별 머지
    eitems = old.get("items", {}) or {}
    nitems = new_data.get("items", {}) or {}
    merged_items = {}
    for hs in set(eitems) | set(nitems):
        eo = eitems.get(hs, {}) or {}
        ne = nitems.get(hs, {}) or {}
        item = dict(ne) if ne else dict(eo)
        if not item.get("name"):
            item["name"] = eo.get("name") or ne.get("name") or hs
        for k in ("total_exp", "total_imp", "total_wgt"):
            merged = _merge_ym_dict(eo.get(k, {}), ne.get(k, {}))
            if merged or k in eo or k in ne:
                item[k] = merged

        # countries
        ec = eo.get("countries", {}) or {}
        nc = ne.get("countries", {}) or {}
        mc = {}
        for cd in set(ec) | set(nc):
            e1 = ec.get(cd, {}) or {}
            n1 = nc.get(cd, {}) or {}
            entry = {
                "name": n1.get("name") or e1.get("name") or cd,
                "exp":  _merge_ym_dict(e1.get("exp", {}), n1.get("exp", {})),
            }
            ew = e1.get("wgt"); nw = n1.get("wgt")
            if ew or nw:
                entry["wgt"] = _merge_ym_dict(ew or {}, nw or {})
            mc[cd] = entry
        item["countries"] = mc

        # regions
        er = eo.get("regions", {}) or {}
        nr = ne.get("regions", {}) or {}
        mr = {}
        for rc in set(er) | set(nr):
            e1 = er.get(rc, {}) or {}
            n1 = nr.get(rc, {}) or {}
            mr[rc] = {
                "name": n1.get("name") or e1.get("name") or rc,
                "exp":  _merge_ym_dict(e1.get("exp", {}), n1.get("exp", {})),
            }
        item["regions"] = mr

        # sub_items
        es = eo.get("sub_items", {}) or {}
        ns = ne.get("sub_items", {}) or {}
        if es or ns:
            ms = {}
            for sc in set(es) | set(ns):
                e1 = es.get(sc, {}) or {}
                n1 = ns.get(sc, {}) or {}
                sub = {
                    "name": n1.get("name") or e1.get("name") or sc,
                    "exp":  _merge_ym_dict(e1.get("exp", {}), n1.get("exp", {})),
                    "wgt":  _merge_ym_dict(e1.get("wgt", {}), n1.get("wgt", {})),
                }
                # sub_items 국가별
                escn = e1.get("countries", {}) or {}
                nscn = n1.get("countries", {}) or {}
                if escn or nscn:
                    mscn = {}
                    for cd in set(escn) | set(nscn):
                        ea = escn.get(cd, {}) or {}
                        na = nscn.get(cd, {}) or {}
                        mscn[cd] = {
                            "name": na.get("name") or ea.get("name") or cd,
                            "exp":  _merge_ym_dict(ea.get("exp", {}), na.get("exp", {})),
                            "wgt":  _merge_ym_dict(ea.get("wgt", {}), na.get("wgt", {})),
                        }
                    sub["countries"] = mscn
                ms[sc] = sub
            item["sub_items"] = ms

        # companies
        eco = eo.get("companies", {}) or {}
        nco = ne.get("companies", {}) or {}
        if eco or nco:
            mco = {}
            for ck in set(eco) | set(nco):
                ev = eco.get(ck, {}) or {}
                nv = nco.get(ck, {}) or {}
                comp = {"name": nv.get("name") or ev.get("name") or ck, "locations": {}}
                el = ev.get("locations", {}) or {}
                nl = nv.get("locations", {}) or {}
                for lk in set(el) | set(nl):
                    e1 = el.get(lk, {}) or {}
                    n1 = nl.get(lk, {}) or {}
                    comp["locations"][lk] = {
                        "name": n1.get("name") or e1.get("name") or lk,
                        "exp":  _merge_ym_dict(e1.get("exp", {}), n1.get("exp", {})),
                    }
                mco[ck] = comp
            item["companies"] = mco

        # samyang (1902 전용)
        esa = eo.get("samyang", {}) or {}
        nsa = ne.get("samyang", {}) or {}
        if esa or nsa:
            msa = {}
            for lk in set(esa) | set(nsa):
                e1 = esa.get(lk, {}) or {}
                n1 = nsa.get(lk, {}) or {}
                msa[lk] = {
                    "name": n1.get("name") or e1.get("name") or lk,
                    "exp":  _merge_ym_dict(e1.get("exp", {}), n1.get("exp", {})),
                }
            item["samyang"] = msa

        merged_items[hs] = item
    out["items"] = merged_items

    # ranking_6d / hs4_names / hs2_names 등 customs_trade_v2.py가 안 만드는 키 보존
    for k in ("ranking_6d", "hs4_names", "hs2_names",
              "ranking_generated_at", "total_generated_at"):
        if k in old and k not in out:
            out[k] = old[k]

    # period: start는 둘 중 더 이른 달, end는 새 데이터 우선
    old_period = old.get("period", {}) or {}
    new_period = new_data.get("period", {}) or {}
    p_start = min(filter(None, [old_period.get("start"), new_period.get("start")]),
                  default=new_period.get("start", ""))
    p_end = new_period.get("end") or old_period.get("end", "")
    out["period"] = {"start": p_start, "end": p_end}

    return out


def update_html(data, html_path="trade.html"):
    """trade.html의 DEMO 데이터를 실제 데이터로 교체"""
    if not os.path.exists(html_path):
        print(f"[WARN] {html_path} 파일을 찾을 수 없습니다.", file=sys.stderr)
        return False

    with open(html_path, "r", encoding="utf-8") as f:
        content = f.read()

    json_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    new_line = f"const DEMO={json_str};"

    pattern = r"const DEMO=\{.*?\};"
    if re.search(pattern, content):
        content = re.sub(pattern, new_line, content)
    else:
        print("[WARN] 'const DEMO=...' 패턴을 찾을 수 없습니다.", file=sys.stderr)
        return False

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"[OK] {html_path} 업데이트 완료")
    return True


def main():
    api_key = os.environ.get("API_KEY", "")
    if not api_key:
        print("ERROR: 환경변수 API_KEY가 설정되지 않았습니다.", file=sys.stderr)
        print("사용법: API_KEY=<인증키> python customs_trade_v2.py", file=sys.stderr)
        sys.exit(1)

    print("=" * 60)
    print("관세청 수출입 데이터 수집 시작")
    print(f"시작 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    data = collect_data(api_key)

    item_count = len(data["items"])
    month_count = len(data["total"]["exp"])
    print(f"\n{'='*60}")
    print(f"이번 수집: {item_count}개 품목, {month_count}개월")

    # 기존 JSON과 머지 (옛 달 보존)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "trade_data_v2.json")
    pre_old_months = 0
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                _old = json.load(f)
            pre_old_months = len((_old.get("total", {}) or {}).get("exp", {}))
        except Exception:
            pass
    data = merge_with_existing(data, json_path)
    merged_months = len(data["total"]["exp"])
    print(f"기존 JSON 머지 후: total {pre_old_months} → {merged_months}개월, "
          f"items {len(data['items'])}개")

    # JSON 저장 (compact: 누적되면 indent로 인한 크기 증가가 커서 separator만)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[OK] {json_path} 저장 완료 ({os.path.getsize(json_path):,} bytes)")

    # HTML 업데이트 (머지 결과 임베드)
    html_path = os.path.join(script_dir, "trade.html")
    update_html(data, html_path)

    print(f"\n완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
