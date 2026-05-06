"""
GeoJSON과 ISO 3166 데이터를 합쳐서 trade.html이 사용할 country_meta.json 생성.

산출 형식:
{
  "geo": {alpha2: {"name3": <ISO alpha-3>, "lng": <centroid lng>, "lat": <centroid lat>}},
  ...
}

centroid는 polygon 좌표의 단순 산술평균(가중치 없음). 시각화 위치 표시용으로 충분.
"""
import json, os, statistics

HERE = os.path.dirname(os.path.abspath(__file__))
GEO = os.path.join(HERE, "world.geo.json")
ISO = os.path.join(HERE, "iso_codes.json")
OUT = os.path.join(HERE, "country_meta.json")

with open(GEO, "r", encoding="utf-8") as f:
    geo = json.load(f)
with open(ISO, "r", encoding="utf-8") as f:
    iso = json.load(f)

# alpha-3 -> alpha-2 (and name)
a3_to_a2 = {}
a2_to_a3 = {}
for c in iso:
    a3 = c.get("alpha-3"); a2 = c.get("alpha-2")
    if a3 and a2:
        a3_to_a2[a3] = a2
        a2_to_a3[a2] = a3

def polygon_centroid(coords):
    """coords: list of [lng,lat] for one ring."""
    xs = [p[0] for p in coords]
    ys = [p[1] for p in coords]
    return statistics.mean(xs), statistics.mean(ys)

def feature_centroid(feat):
    g = feat.get("geometry") or {}
    t = g.get("type")
    cs = g.get("coordinates")
    if not cs:
        return None
    if t == "Polygon":
        # outer ring
        return polygon_centroid(cs[0])
    if t == "MultiPolygon":
        # 각 폴리곤의 면적 가중치 없이 가장 큰(점 많은) 폴리곤 외곽 중심 사용
        best = max(cs, key=lambda poly: len(poly[0]))
        return polygon_centroid(best[0])
    return None

result = {}
for feat in geo.get("features", []):
    a3 = feat.get("id")
    if not a3:
        continue
    a2 = a3_to_a2.get(a3)
    if not a2:
        continue
    c = feature_centroid(feat)
    if not c:
        continue
    name = (feat.get("properties") or {}).get("name") or a2
    result[a2] = {"a3": a3, "name": name, "lng": round(c[0], 4), "lat": round(c[1], 4)}

# GeoJSON에 없는 일부 ISO alpha-2 국가에 대한 수동 좌표 보강
# (작은 국가, 분쟁지역, 실효 영토 등 — TRASS 데이터에 자주 등장하는 코드 위주)
EXTRA = {
    "TW": {"a3": "TWN", "name": "Taiwan", "lng": 121.0, "lat": 23.7},
    "HK": {"a3": "HKG", "name": "Hong Kong", "lng": 114.17, "lat": 22.32},
    "MO": {"a3": "MAC", "name": "Macao", "lng": 113.55, "lat": 22.20},
    "SG": {"a3": "SGP", "name": "Singapore", "lng": 103.82, "lat": 1.35},
    "BH": {"a3": "BHR", "name": "Bahrain", "lng": 50.55, "lat": 26.07},
    "MT": {"a3": "MLT", "name": "Malta", "lng": 14.5, "lat": 35.9},
    "MV": {"a3": "MDV", "name": "Maldives", "lng": 73.22, "lat": 3.20},
    "MU": {"a3": "MUS", "name": "Mauritius", "lng": 57.55, "lat": -20.35},
    "AD": {"a3": "AND", "name": "Andorra", "lng": 1.52, "lat": 42.55},
    "MC": {"a3": "MCO", "name": "Monaco", "lng": 7.42, "lat": 43.74},
    "LI": {"a3": "LIE", "name": "Liechtenstein", "lng": 9.55, "lat": 47.16},
    "SM": {"a3": "SMR", "name": "San Marino", "lng": 12.46, "lat": 43.94},
    "VA": {"a3": "VAT", "name": "Vatican", "lng": 12.45, "lat": 41.90},
    "BB": {"a3": "BRB", "name": "Barbados", "lng": -59.55, "lat": 13.18},
    "AG": {"a3": "ATG", "name": "Antigua and Barbuda", "lng": -61.79, "lat": 17.06},
    "DM": {"a3": "DMA", "name": "Dominica", "lng": -61.37, "lat": 15.41},
    "GD": {"a3": "GRD", "name": "Grenada", "lng": -61.68, "lat": 12.11},
    "KN": {"a3": "KNA", "name": "Saint Kitts and Nevis", "lng": -62.78, "lat": 17.35},
    "LC": {"a3": "LCA", "name": "Saint Lucia", "lng": -61.0, "lat": 13.9},
    "VC": {"a3": "VCT", "name": "Saint Vincent and the Grenadines", "lng": -61.21, "lat": 13.25},
    "TT": {"a3": "TTO", "name": "Trinidad and Tobago", "lng": -61.22, "lat": 10.69},
    "FM": {"a3": "FSM", "name": "Micronesia", "lng": 158.21, "lat": 6.92},
    "MH": {"a3": "MHL", "name": "Marshall Islands", "lng": 171.18, "lat": 7.13},
    "PW": {"a3": "PLW", "name": "Palau", "lng": 134.58, "lat": 7.51},
    "KI": {"a3": "KIR", "name": "Kiribati", "lng": -168.73, "lat": -3.37},
    "TV": {"a3": "TUV", "name": "Tuvalu", "lng": 179.20, "lat": -7.48},
    "NR": {"a3": "NRU", "name": "Nauru", "lng": 166.93, "lat": -0.52},
    "TO": {"a3": "TON", "name": "Tonga", "lng": -175.20, "lat": -21.18},
    "WS": {"a3": "WSM", "name": "Samoa", "lng": -172.10, "lat": -13.76},
    "SC": {"a3": "SYC", "name": "Seychelles", "lng": 55.49, "lat": -4.68},
    "ST": {"a3": "STP", "name": "Sao Tome and Principe", "lng": 6.61, "lat": 0.19},
    "KM": {"a3": "COM", "name": "Comoros", "lng": 43.34, "lat": -11.65},
    "BN": {"a3": "BRN", "name": "Brunei", "lng": 114.73, "lat": 4.54},
    "BT": {"a3": "BTN", "name": "Bhutan", "lng": 90.43, "lat": 27.51},
    # 영토/속령
    "PR": {"a3": "PRI", "name": "Puerto Rico", "lng": -66.59, "lat": 18.22},
    "GU": {"a3": "GUM", "name": "Guam", "lng": 144.79, "lat": 13.44},
    "VI": {"a3": "VIR", "name": "U.S. Virgin Islands", "lng": -64.84, "lat": 18.34},
    "MP": {"a3": "MNP", "name": "Northern Mariana Islands", "lng": 145.74, "lat": 15.10},
    "AS": {"a3": "ASM", "name": "American Samoa", "lng": -170.7, "lat": -14.27},
    "BM": {"a3": "BMU", "name": "Bermuda", "lng": -64.78, "lat": 32.32},
    "KY": {"a3": "CYM", "name": "Cayman Islands", "lng": -81.27, "lat": 19.31},
    "VG": {"a3": "VGB", "name": "British Virgin Islands", "lng": -64.62, "lat": 18.42},
    "TC": {"a3": "TCA", "name": "Turks and Caicos", "lng": -71.80, "lat": 21.69},
    "AI": {"a3": "AIA", "name": "Anguilla", "lng": -63.07, "lat": 18.22},
    "MS": {"a3": "MSR", "name": "Montserrat", "lng": -62.19, "lat": 16.74},
    "BL": {"a3": "BLM", "name": "Saint Barthelemy", "lng": -62.83, "lat": 17.90},
    "MF": {"a3": "MAF", "name": "Saint Martin", "lng": -63.05, "lat": 18.08},
    "SX": {"a3": "SXM", "name": "Sint Maarten", "lng": -63.07, "lat": 18.04},
    "AW": {"a3": "ABW", "name": "Aruba", "lng": -69.97, "lat": 12.52},
    "CW": {"a3": "CUW", "name": "Curacao", "lng": -69.0, "lat": 12.17},
    "BQ": {"a3": "BES", "name": "Caribbean Netherlands", "lng": -68.27, "lat": 12.18},
    "MQ": {"a3": "MTQ", "name": "Martinique", "lng": -61.02, "lat": 14.64},
    "GP": {"a3": "GLP", "name": "Guadeloupe", "lng": -61.55, "lat": 16.27},
    "RE": {"a3": "REU", "name": "Reunion", "lng": 55.54, "lat": -21.11},
    "YT": {"a3": "MYT", "name": "Mayotte", "lng": 45.16, "lat": -12.83},
    "GF": {"a3": "GUF", "name": "French Guiana", "lng": -53.13, "lat": 3.93},
    "PM": {"a3": "SPM", "name": "Saint Pierre and Miquelon", "lng": -56.33, "lat": 46.94},
    "PF": {"a3": "PYF", "name": "French Polynesia", "lng": -149.41, "lat": -17.68},
    "NC": {"a3": "NCL", "name": "New Caledonia", "lng": 165.62, "lat": -20.90},
    "WF": {"a3": "WLF", "name": "Wallis and Futuna", "lng": -177.16, "lat": -13.77},
    "CK": {"a3": "COK", "name": "Cook Islands", "lng": -159.78, "lat": -21.24},
    "NU": {"a3": "NIU", "name": "Niue", "lng": -169.87, "lat": -19.05},
    "TK": {"a3": "TKL", "name": "Tokelau", "lng": -171.85, "lat": -8.97},
    "JE": {"a3": "JEY", "name": "Jersey", "lng": -2.13, "lat": 49.21},
    "GG": {"a3": "GGY", "name": "Guernsey", "lng": -2.58, "lat": 49.45},
    "IM": {"a3": "IMN", "name": "Isle of Man", "lng": -4.55, "lat": 54.24},
    "AX": {"a3": "ALA", "name": "Aland Islands", "lng": 19.93, "lat": 60.18},
    "FO": {"a3": "FRO", "name": "Faroe Islands", "lng": -6.91, "lat": 61.89},
    "GI": {"a3": "GIB", "name": "Gibraltar", "lng": -5.35, "lat": 36.14},
    "GL": {"a3": "GRL", "name": "Greenland", "lng": -42.0, "lat": 71.7},
    # 분쟁/기타
    "PS": {"a3": "PSE", "name": "Palestine", "lng": 35.23, "lat": 31.95},
    "XK": {"a3": "XKX", "name": "Kosovo", "lng": 20.90, "lat": 42.60},
    "MK": {"a3": "MKD", "name": "North Macedonia", "lng": 21.7, "lat": 41.6},
    "TL": {"a3": "TLS", "name": "Timor-Leste", "lng": 125.73, "lat": -8.87},
    "SS": {"a3": "SSD", "name": "South Sudan", "lng": 31.31, "lat": 6.88},
    # 주요 국가 누락 보정 (GeoJSON에 없을 경우 대비)
    "VU": {"a3": "VUT", "name": "Vanuatu", "lng": 168.32, "lat": -16.0},
    "FJ": {"a3": "FJI", "name": "Fiji", "lng": 178.07, "lat": -17.71},
    "SB": {"a3": "SLB", "name": "Solomon Islands", "lng": 160.16, "lat": -9.65},
    "PG": {"a3": "PNG", "name": "Papua New Guinea", "lng": 143.96, "lat": -6.32},
    "CV": {"a3": "CPV", "name": "Cape Verde", "lng": -23.61, "lat": 16.54},
    "GQ": {"a3": "GNQ", "name": "Equatorial Guinea", "lng": 10.27, "lat": 1.65},
    "DJ": {"a3": "DJI", "name": "Djibouti", "lng": 42.59, "lat": 11.83},
    "GW": {"a3": "GNB", "name": "Guinea-Bissau", "lng": -14.50, "lat": 11.80},
    "SZ": {"a3": "SWZ", "name": "Eswatini", "lng": 31.47, "lat": -26.52},
    "LS": {"a3": "LSO", "name": "Lesotho", "lng": 28.23, "lat": -29.61},
    "ER": {"a3": "ERI", "name": "Eritrea", "lng": 39.78, "lat": 15.18},
    "BW": {"a3": "BWA", "name": "Botswana", "lng": 24.68, "lat": -22.33},
    "NA": {"a3": "NAM", "name": "Namibia", "lng": 18.49, "lat": -22.96},
    "MG": {"a3": "MDG", "name": "Madagascar", "lng": 46.87, "lat": -18.77},
    "AO": {"a3": "AGO", "name": "Angola", "lng": 17.87, "lat": -11.20},
    "MZ": {"a3": "MOZ", "name": "Mozambique", "lng": 35.53, "lat": -18.67},
    "MW": {"a3": "MWI", "name": "Malawi", "lng": 34.30, "lat": -13.25},
    "ZW": {"a3": "ZWE", "name": "Zimbabwe", "lng": 29.15, "lat": -19.02},
    "ZM": {"a3": "ZMB", "name": "Zambia", "lng": 27.85, "lat": -13.13},
    "TZ": {"a3": "TZA", "name": "Tanzania", "lng": 34.89, "lat": -6.37},
    "UG": {"a3": "UGA", "name": "Uganda", "lng": 32.29, "lat": 1.37},
    "RW": {"a3": "RWA", "name": "Rwanda", "lng": 29.87, "lat": -1.94},
    "BI": {"a3": "BDI", "name": "Burundi", "lng": 29.92, "lat": -3.37},
    "KE": {"a3": "KEN", "name": "Kenya", "lng": 37.91, "lat": 0.02},
    "ET": {"a3": "ETH", "name": "Ethiopia", "lng": 40.49, "lat": 9.15},
    "SO": {"a3": "SOM", "name": "Somalia", "lng": 46.20, "lat": 5.15},
    "SD": {"a3": "SDN", "name": "Sudan", "lng": 30.22, "lat": 12.86},
    "TD": {"a3": "TCD", "name": "Chad", "lng": 18.73, "lat": 15.45},
    "LY": {"a3": "LBY", "name": "Libya", "lng": 17.23, "lat": 26.34},
    "EG": {"a3": "EGY", "name": "Egypt", "lng": 30.80, "lat": 26.82},
    "TN": {"a3": "TUN", "name": "Tunisia", "lng": 9.54, "lat": 33.89},
    "DZ": {"a3": "DZA", "name": "Algeria", "lng": 1.66, "lat": 28.03},
    "MA": {"a3": "MAR", "name": "Morocco", "lng": -7.09, "lat": 31.79},
    "MR": {"a3": "MRT", "name": "Mauritania", "lng": -10.94, "lat": 21.00},
    "SN": {"a3": "SEN", "name": "Senegal", "lng": -14.45, "lat": 14.50},
    "GM": {"a3": "GMB", "name": "Gambia", "lng": -15.31, "lat": 13.44},
    "ML": {"a3": "MLI", "name": "Mali", "lng": -3.99, "lat": 17.57},
    "BF": {"a3": "BFA", "name": "Burkina Faso", "lng": -1.56, "lat": 12.24},
    "NE": {"a3": "NER", "name": "Niger", "lng": 8.08, "lat": 17.61},
    "NG": {"a3": "NGA", "name": "Nigeria", "lng": 8.68, "lat": 9.08},
    "BJ": {"a3": "BEN", "name": "Benin", "lng": 2.32, "lat": 9.31},
    "TG": {"a3": "TGO", "name": "Togo", "lng": 0.83, "lat": 8.62},
    "GH": {"a3": "GHA", "name": "Ghana", "lng": -1.02, "lat": 7.95},
    "CI": {"a3": "CIV", "name": "Cote d'Ivoire", "lng": -5.55, "lat": 7.54},
    "LR": {"a3": "LBR", "name": "Liberia", "lng": -9.43, "lat": 6.43},
    "SL": {"a3": "SLE", "name": "Sierra Leone", "lng": -11.78, "lat": 8.46},
    "GN": {"a3": "GIN", "name": "Guinea", "lng": -9.71, "lat": 9.95},
    "CM": {"a3": "CMR", "name": "Cameroon", "lng": 12.35, "lat": 7.37},
    "CF": {"a3": "CAF", "name": "Central African Republic", "lng": 20.94, "lat": 6.61},
    "GA": {"a3": "GAB", "name": "Gabon", "lng": 11.78, "lat": -0.80},
    "CG": {"a3": "COG", "name": "Republic of Congo", "lng": 15.83, "lat": -0.83},
    "CD": {"a3": "COD", "name": "DR Congo", "lng": 23.66, "lat": -2.88},
}
for k, v in EXTRA.items():
    if k not in result:
        result[k] = v

# 결과 정렬해서 저장 (가독성)
out = {k: result[k] for k in sorted(result.keys())}
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)
print(f"저장: {OUT}")
print(f"총 {len(out)}개 alpha-2 국가 좌표")
