"""DB에서 trade.html이 기대하는 JSON 구조 재조립"""
import json
from collections import defaultdict
from .database import get_connection


def build_full_json() -> dict:
    """trade_data_v2.json과 동일한 구조의 dict 반환"""
    conn = get_connection()
    result = {}

    # ── 1) 메타데이터 ──
    meta = {r["key"]: r["value"]
            for r in conn.execute("SELECT key, value FROM meta")}
    result["generated_at"] = meta.get("generated_at", "")
    result["period"] = {
        "start": meta.get("period_start", ""),
        "end": meta.get("period_end", ""),
    }
    result["main_items"] = json.loads(meta.get("main_items", "[]"))
    result["sub_items_def"] = json.loads(meta.get("sub_items_def", "{}"))

    # ── 2) 사전 데이터 ──
    result["all_countries"] = {
        r["code"]: r["name"]
        for r in conn.execute("SELECT code, name FROM countries")
    }
    result["all_regions"] = {
        r["code"]: r["name"]
        for r in conn.execute("SELECT code, name FROM regions")
    }
    result["hs4_names"] = {
        r["hs_code"]: r["name"]
        for r in conn.execute("SELECT hs_code, name FROM hs_names WHERE digits=4")
    }
    result["hs2_names"] = {
        r["hs_code"]: r["name"]
        for r in conn.execute("SELECT hs_code, name FROM hs_names WHERE digits=2")
    }

    # ── 3) 전체 총계 ──
    total_exp, total_imp = {}, {}
    for r in conn.execute(
            "SELECT ym, exp_usd, imp_usd FROM trade_data WHERE data_type='total'"):
        total_exp[r["ym"]] = r["exp_usd"]
        total_imp[r["ym"]] = r["imp_usd"]
    result["total"] = {"exp": total_exp, "imp": total_imp}

    # ── 4) 품목 데이터 ──
    # 모든 trade_data를 한번에 읽어 메모리에서 분류 (쿼리 횟수 최소화)
    all_td = defaultdict(list)
    for r in conn.execute(
            "SELECT data_type, hs_code, sub_code, entity_code, ym, exp_usd, imp_usd, wgt "
            "FROM trade_data WHERE data_type != 'total' AND data_type != 'ranking'"):
        all_td[(r["data_type"], r["hs_code"])].append(r)

    # 세부항목 정의
    all_subs = defaultdict(dict)
    for r in conn.execute("SELECT hs_code, sub_code, name FROM sub_items"):
        all_subs[r["hs_code"]][r["sub_code"]] = r["name"]

    # 기업 정의
    all_companies = defaultdict(dict)
    for r in conn.execute("SELECT hs_code, company_key, name FROM companies"):
        all_companies[r["hs_code"]][r["company_key"]] = r["name"]

    # 기업 사업장
    all_locs = defaultdict(lambda: defaultdict(dict))
    for r in conn.execute(
            "SELECT hs_code, company_key, location_key, name FROM company_locations"):
        all_locs[r["hs_code"]][r["company_key"]][r["location_key"]] = r["name"]

    items_dict = {}
    for item_row in conn.execute(
            "SELECT hs_code, name FROM items ORDER BY sort_order"):
        hs = item_row["hs_code"]
        item = {"name": item_row["name"]}

        # 품목 총계
        item["total_exp"] = {}
        item["total_imp"] = {}
        total_wgt = {}
        for r in all_td.get(("item", hs), []):
            item["total_exp"][r["ym"]] = r["exp_usd"]
            item["total_imp"][r["ym"]] = r["imp_usd"]
            if r["wgt"]:
                total_wgt[r["ym"]] = r["wgt"]
        if total_wgt:
            item["total_wgt"] = total_wgt

        # 국가별
        countries = defaultdict(lambda: {"name": "", "exp": {}})
        has_country_wgt = False
        for r in all_td.get(("item_country", hs), []):
            cd = r["entity_code"]
            countries[cd]["exp"][r["ym"]] = r["exp_usd"]
            if r["wgt"]:
                if "wgt" not in countries[cd]:
                    countries[cd]["wgt"] = {}
                countries[cd]["wgt"][r["ym"]] = r["wgt"]
                has_country_wgt = True
        for cd in countries:
            countries[cd]["name"] = result["all_countries"].get(cd, cd)
        item["countries"] = dict(countries)

        # 지역별
        regions = defaultdict(lambda: {"name": "", "exp": {}})
        for r in all_td.get(("item_region", hs), []):
            cd = r["entity_code"]
            regions[cd]["exp"][r["ym"]] = r["exp_usd"]
        for cd in regions:
            regions[cd]["name"] = result["all_regions"].get(cd, cd)
        item["regions"] = dict(regions)

        # 세부항목
        if hs in all_subs:
            sub_items = {}
            for scode, sname in all_subs[hs].items():
                si = {"name": sname, "exp": {}, "wgt": {}}
                for r in all_td.get(("sub_item", hs), []):
                    if r["sub_code"] == scode:
                        si["exp"][r["ym"]] = r["exp_usd"]
                        if r["wgt"]:
                            si["wgt"][r["ym"]] = r["wgt"]
                # 세부항목 국가별
                si_countries = defaultdict(lambda: {"name": "", "exp": {}, "wgt": {}})
                for r in all_td.get(("sub_country", hs), []):
                    if r["sub_code"] == scode:
                        cd = r["entity_code"]
                        si_countries[cd]["exp"][r["ym"]] = r["exp_usd"]
                        if r["wgt"]:
                            si_countries[cd]["wgt"][r["ym"]] = r["wgt"]
                for cd in si_countries:
                    si_countries[cd]["name"] = result["all_countries"].get(cd, cd)
                si["countries"] = dict(si_countries)
                sub_items[scode] = si
            item["sub_items"] = sub_items

        # 기업별
        if hs in all_companies:
            companies = {}
            for ck, cname in all_companies[hs].items():
                # samyang은 별도 처리
                if hs == "1902" and ck == "samyang":
                    continue
                comp = {"name": cname, "locations": {}}
                for lk, lname in all_locs.get(hs, {}).get(ck, {}).items():
                    loc_exp = {}
                    for r in all_td.get(("company_loc", hs), []):
                        if r["sub_code"] == ck and r["entity_code"] == lk:
                            loc_exp[r["ym"]] = r["exp_usd"]
                    comp["locations"][lk] = {"name": lname, "exp": loc_exp}
                companies[ck] = comp
            if companies:
                item["companies"] = companies

        # samyang (1902 전용)
        if hs == "1902" and "samyang" in all_companies.get(hs, {}):
            samyang = {}
            for lk, lname in all_locs.get(hs, {}).get("samyang", {}).items():
                loc_exp = {}
                for r in all_td.get(("company_loc", hs), []):
                    if r["sub_code"] == "samyang" and r["entity_code"] == lk:
                        loc_exp[r["ym"]] = r["exp_usd"]
                samyang[lk] = {"name": lname, "exp": loc_exp}
            if samyang:
                item["samyang"] = samyang

        items_dict[hs] = item

    result["items"] = items_dict

    # ── 5) 랭킹 ──
    # hs6 이름 사전
    hs6_names = {}
    for r in conn.execute("SELECT hs_code, name FROM hs_names WHERE digits=6"):
        hs6_names[r["hs_code"]] = r["name"]

    ranking = defaultdict(lambda: {"name": "", "exp": {}})
    for r in conn.execute(
            "SELECT sub_code, ym, exp_usd FROM trade_data WHERE data_type='ranking'"):
        hs6 = r["sub_code"]
        ranking[hs6]["exp"][r["ym"]] = r["exp_usd"]
    for hs6 in ranking:
        ranking[hs6]["name"] = hs6_names.get(hs6, "")
    result["ranking_6d"] = dict(ranking)

    conn.close()
    return result
