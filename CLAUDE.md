# 수출입 대시보드 프로젝트 인수인계

## 프로젝트 개요
관세청 OpenAPI 기반 수출입 데이터 대시보드. FastAPI + SQLite + 단일 HTML 프론트엔드.

- **라이브 URL**: https://trade-dashboard-z0t4.onrender.com
- **GitHub repo**: https://github.com/RyannRonn/trade-dashboard
- **GitHub 계정**: RyannRonn
- **호스팅**: Render 무료 tier (Singapore 리전, 15분 미사용 시 슬립)

## 기술 스택
- Python 3.9+ / FastAPI / uvicorn / SQLite
- 프론트엔드: `trade.html` (단일 파일, Chart.js CDN)
- DB: `trade.db` (SQLite, ~9.4MB, 79,732행)
- 배포: Docker → Render (GitHub push 시 자동 재배포)

## 파일 구조
```
trade-dashboard/
├── server/                  # FastAPI 서버
│   ├── main.py              # 엔트리포인트 (3개 엔드포인트: /, /api/trade-data, /api/health)
│   ├── config.py            # 경로 설정 (BASE_DIR, DB_PATH 등)
│   ├── database.py          # SQLite 스키마, 인덱스, PRAGMA 설정
│   ├── builder.py           # DB → JSON 변환 (trade.html이 기대하는 구조)
│   └── __init__.py
├── collector/
│   └── migrate_json.py      # JSON → DB 마이그레이션
├── trade.db                 # SQLite 데이터베이스 (주 저장소)
├── trade.html               # 프론트엔드 (단일 파일, ~2.2MB)
├── trade_data_v2.json       # JSON 백업 (~2.2MB)
├── customs_trade_v2.py      # 메인 데이터 수집 스크립트
├── collect_ranking.py       # 급등/급락 6자리 HS 전수 수집
├── collect_botox.py         # 보톡스/필러 수집
├── collect_cosmetics.py     # 화장품 수집
├── collect_electric.py      # 전력 수집
├── collect_hfs.py           # 건기식 수집
├── collect_medbeauty.py     # 미용의료기기 수집
├── sync_demo.py             # JSON → HTML DEMO 데이터 동기화
├── Dockerfile               # Docker 빌드 (python:3.11-slim)
├── render.yaml              # Render Blueprint 설정
├── requirements.txt         # Python 의존성
└── .github/workflows/
    └── trade-update.yml     # 매월 15일 자동 데이터 수집
```

## 로컬 실행
```bash
pip install -r requirements.txt
python -m server.main
# → http://127.0.0.1:8000
```

## 배포 흐름
```
로컬 수정 → git push → GitHub → Render 자동 재배포 (1~2분)
```

## 데이터 수집 흐름
- GitHub Actions: 매월 15일 09:00 KST 자동 실행
- 수동 실행: GitHub Actions 페이지 → Run workflow
- 수집 후: trade.html + trade_data_v2.json + trade.db 자동 commit → Render 재배포

## 품목 구성 (7개 고정 탭 + 급등/급락 탭)
1. 반도체 (HS 8541)
2. 전력 (HS 8507)
3. 라면 (HS 1902)
4. 화장품 (HS 3304)
5. 미용의료기기 (HS 9018)
6. 보톡스/필러 (HS 3004)
7. 건기식 (HS 2106)
8. 급등/급락 탭: 6자리 HS 5,431개 품목, YoY/MoM 기준 정렬

## DB 구조
- 핵심 테이블: `trade_data` (data_type으로 구분)
  - data_type: total / item / item_country / item_region / sub_item / sub_country / company_loc / ranking
- 인덱스 4개: idx_trade_ym, idx_trade_type_hs, idx_trade_type_hs_sub, idx_hs_names_digits
- PRAGMA: cache_size=20MB, mmap_size=50MB, temp_store=MEMORY

## API 정보
- **API Key**: `a1638c66a4679da0c21bc0ac0daf82478b937899da84d2e71e8dd2faab54acb1`
- nitemtrade: 품목×국가 (expDlr = USD 단위)
- sigungu: 시군구×품목 (expUsdAmt = 천USD, ×1000 필요)
- sidoitemtrade: 시도×품목 (expUsdAmt = 천USD)
- 기간 제한: 최대 12개월/호출

## UI 규칙
- 한국어 UI, 코드 주석도 한국어
- 차트 우선, 테이블은 차트 아래
- 테이블 월 순서: 최신→과거 (좌→우), 최신월 밝게 강조
- 탭/버튼: 가운데 정렬, 제목: 왼쪽 정렬
- 금액 단위: USD (천USD 아님)
- 기존 잘 되던 탭 건들지 말 것

## 다음 작업 (검토 중)
- **동적 탭 추가** — 매달 내용이 바뀌는 탭. 후보:
  1. **월간 브리핑 (추천)** — 총 수출액 요약, 성장/하락 섹터 TOP5, 주목 품목 자동 선정
  2. 연속 성장 품목 — 3개월+ 연속 YoY 성장 중인 품목
  3. 섹터 히트맵 — 96개 대분류(2자리 HS) 격자, 성장률별 색상
  4. 신흥 수출품 — 전년 거의 0 → 올해 급증한 품목
  - 모두 기존 ranking_6d 데이터로 구현 가능 (추가 수집 불필요)
- 수집 스크립트 DB 직접 쓰기 전환
- 엑셀 업로드 기능
- 증분 수집
