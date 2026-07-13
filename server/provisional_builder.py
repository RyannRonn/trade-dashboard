"""prov_* 테이블 → provisional.html이 기대하는 중첩 dict 재조립

확정치 builder.py와 같은 패턴. 원본 provisional_data.json과 semantic 동치인
구조를 만든다:
    { 품목키: {h, d, u, s:{국가:{YYYYMM:{cut:{c,v,w,a}}}}} }

핵심:
  - 품목 순서는 prov_items.sort_order, 국가 순서는 prov_countries.sort_order로 보존
    (프론트가 Object.keys 삽입순서에 의존 → 탭/섹션 버튼 순서)
  - leaf는 값이 NULL이 아닌 키만 emit (부재 v/w를 0으로 되살리지 않음)
"""
from collections import defaultdict
from .database import get_connection


def build_provisional_json() -> dict:
    conn = get_connection()

    # 국가 순서: (item_key, country) → sort_order
    country_order = {}
    for r in conn.execute(
            "SELECT item_key, country, sort_order FROM prov_countries"):
        country_order[(r["item_key"], r["country"])] = r["sort_order"]

    # 전체 시계열을 한 번에 읽어 메모리에서 조립 (쿼리 최소화)
    #   grouped[item_key][country][ym][cut] = {비-NULL leaf만}
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for r in conn.execute(
            "SELECT item_key, country, ym, cut, c, v, w, a FROM prov_data"):
        leaf = {}
        if r["c"] is not None:
            leaf["c"] = r["c"]
        if r["v"] is not None:
            leaf["v"] = r["v"]
        if r["w"] is not None:
            leaf["w"] = r["w"]
        if r["a"] is not None:
            leaf["a"] = r["a"]
        grouped[r["item_key"]][r["country"]][r["ym"]][r["cut"]] = leaf

    result = {}
    for item in conn.execute(
            "SELECT item_key, h, d, u FROM prov_items ORDER BY sort_order"):
        ikey = item["item_key"]
        # 국가를 sort_order 순으로 정렬해 삽입 (섹션 버튼 순서 보존)
        countries = grouped.get(ikey, {})
        ordered_countries = sorted(
            countries.keys(),
            key=lambda c: country_order.get((ikey, c), 1 << 30))
        s = {country: dict(countries[country]) for country in ordered_countries}
        result[ikey] = {
            "h": item["h"],
            "d": item["d"],
            "u": item["u"],
            "s": s,
        }

    conn.close()
    return result
