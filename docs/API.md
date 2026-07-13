# 수출입 대시보드 데이터 API 계약 (v1)

이 문서는 **프론트UI ↔ 데이터API 사이의 계약**을 명세한다. 대시보드를 다른
웹사이트로 편입할 때, 프론트는 여기 정의된 엔드포인트만 알면 되고 수집/DB 계층은
자유롭게 교체할 수 있다. (3계층 분리: ①수집 → ②데이터API(=편입 지점) → ③프론트UI)

- **Base URL (라이브)**: `https://trade-dashboard-z0t4.onrender.com`
- **CORS**: 모든 오리진 GET 허용 (`Access-Control-Allow-Origin: *`)
- **캐시**: 각 엔드포인트는 `trade.db` 파일 mtime 기준 인메모리 캐시. DB 갱신 시 자동 무효화.

---

## `GET /api/trade-data`  — 확정치

`trade.html`이 소비하는 완전한 확정치 구조. `trade_data_v2.json`과 동일 스키마.
DB(`trade_data` 등) + 일부 JSON override(`ranking_6d`, `total`)를 병합해 생성.
상세 스키마는 `server/builder.py` 참조.

## `GET /api/provisional-data`  — 잠정치 (10/20/30일 누적)

`provisional.html`이 소비. 정적 `provisional_data.json`과 **semantic 동치**.
프론트는 이 엔드포인트를 먼저 시도하고, 실패 시 정적 JSON으로 폴백한다
(백엔드 없는 GitHub Pages 배포도 계속 동작).

**응답 구조**

```jsonc
{
  "<품목키>": {                 // 예: "보톡스", "라면(원화)", "반도체 _낸드"
    "h": "3002491000",         // HS 코드
    "d": "수출",                // 구분
    "u": "천$",                 // 단위 ("천$" | "백만원")
    "s": {                      // 국가/구분별 (키 순서 = 섹션 버튼 순서, 보통 "전세계" 우선)
      "전세계": {
        "202607": {             // YYYYMM
          "10": { "c": 8150.0, "v": 8150.0, "a": 8150.0, "w": 12.3 },
          "20": { ... },
          "30": { ... }
        }
        // ... 다른 월
      }
      // ... 다른 국가
    }
  }
  // ... 다른 품목 (키 순서 = 탭 순서)
}
```

**leaf 필드 (누적 기준값)**

| 키 | 의미 | 존재성 |
|----|------|--------|
| `c` | 누적 수출액 (해당 cut까지) | 전 레코드 존재 |
| `a` | 3주(구간) 이동평균 보조값 | 전 레코드 존재 |
| `v` | 해당 10일 구간 증분 수출액 | **일부만** — 없으면 키 생략 |
| `w` | 누적 중량(톤) | **일부만** — 없으면 키 생략 |

- **부재 = 키 자체가 없음** (NULL을 0으로 채우지 않는다). 프론트는 `?? null`로 처리.
- 값은 int/float 혼재 가능 (JSON 파싱 후 동일 수치).
- `cut` ∈ `{"10","20","30"}` = 각 월의 ~10일/~20일/~30일 누적.

## `GET /api/health`

`{ "status": "ok", "db_exists": true }`

---

## 정적 폴백 라우트 (백엔드 없는 배포용)

프론트 HTML과 함께 서빙되는 원본 파일. API가 없을 때 프론트가 직접 fetch.

- `GET /provisional_data.json` · `GET /business_days.json` · `GET /confirmed_companies.json`

## 데이터 생성 파이프라인 (참고, 계약 밖)

```
수집 스크립트 → *.json → collector/migrate_*.py → trade.db → server/*_builder.py → API
```

- 확정치: `trade_data_v2.json` → `collector/migrate_json.py` → `server/builder.py`
- 잠정치: `provisional_data.json` → `collector/migrate_provisional.py` → `server/provisional_builder.py`

빌드 시 `trade.db`를 JSON에서 재생성 (Dockerfile의 `RUN python -m collector.migrate_*`).
