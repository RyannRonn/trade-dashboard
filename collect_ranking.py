#!/usr/bin/env python3
"""
급등/급락 6자리 HS 전수 수집 (증분 방식)
- 4자리 HS 코드로 API 호출 → 6자리 hsCd 추출
- 이미 수집된 월은 건너뛰고 최신 월만 수집
- trade_data_v2.json의 "ranking_6d" 키에 저장
"""
import os, sys, json, time, io
from datetime import datetime
from collections import defaultdict

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
    """4자리 HS 코드 1개 호출 → 6자리별 월별 수출액+품목명 추출"""
    items_6d = defaultdict(lambda: {"name": "", "exp": {}})

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
            items_6d[hc]["exp"][ym] = items_6d[hc]["exp"].get(ym, 0) + exp
            # 품목명: statKor 첫 번째 유효값 사용
            if not items_6d[hc]["name"]:
                nm = r.get("statKor", "").strip()
                if nm and nm != "-":
                    items_6d[hc]["name"] = nm

    return dict(items_6d)


def main():
    if not API_KEY:
        print("ERROR: API_KEY 환경변수 필요", file=sys.stderr)
        sys.exit(1)

    # 기존 JSON 로드
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trade_data_v2.json")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 기존 ranking_6d 데이터
    ranking = data.get("ranking_6d", {})

    # 기존 데이터에서 수집 완료된 월 확인 (아무 항목이나 하나 확인)
    existing_months = set()
    for item_data in ranking.values():
        existing_months.update(item_data.get("exp", {}).keys())

    # 수집할 월 계산
    missing = get_months_to_collect(existing_months)
    if not missing:
        print("모든 월이 이미 수집되어 있습니다. 종료.")
        return

    print(f"기존 수집 월: {len(existing_months)}개")
    print(f"신규 수집 월: {missing}")

    date_ranges = make_ranges(missing)
    print(f"API 구간: {date_ranges}")

    # 4자리 HS 코드 목록
    hs4_list = get_all_hs4()
    total = len(hs4_list)
    print(f"\n4자리 HS 코드 {total}개 수집 시작...\n")

    new_count = 0
    start_time = time.time()

    for idx, hs4 in enumerate(hs4_list):
        batch = collect_hs4_batch(hs4, API_KEY, date_ranges)

        for hs6, new_data in batch.items():
            if hs6 not in ranking:
                ranking[hs6] = {"name": new_data["name"], "exp": {}}
                new_count += 1
            # 품목명 업데이트 (기존에 없으면)
            if not ranking[hs6].get("name") and new_data["name"]:
                ranking[hs6]["name"] = new_data["name"]
            # 월별 데이터 merge
            for ym, val in new_data["exp"].items():
                ranking[hs6]["exp"][ym] = val

        # 진행률 표시
        if (idx + 1) % 100 == 0 or idx == total - 1:
            elapsed = time.time() - start_time
            pct = (idx + 1) / total * 100
            eta = elapsed / (idx + 1) * (total - idx - 1)
            print(f"  [{idx+1}/{total}] {pct:.0f}% — {elapsed:.0f}s 경과, 잔여 {eta:.0f}s")

        time.sleep(REQUEST_DELAY)

    elapsed = time.time() - start_time
    print(f"\n수집 완료: {elapsed:.0f}초")
    print(f"총 6자리 항목: {len(ranking)}개 (신규 {new_count}개)")

    # 저장
    data["ranking_6d"] = ranking
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"trade_data_v2.json 저장 완료 ({os.path.getsize(json_path):,} bytes)")
    print("DONE")


if __name__ == "__main__":
    main()
