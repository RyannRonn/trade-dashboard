"""FastAPI 서버: trade.html에 데이터 제공"""
import os
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .config import BASE_DIR, DB_PATH
from .builder import build_full_json
from .provisional_builder import build_provisional_json
from .database import init_db

app = FastAPI(title="수출입 대시보드 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# 캐시: DB 파일 수정 시간 기준
_cache = {"data": None, "mtime": 0}
_prov_cache = {"data": None, "mtime": 0}


@app.on_event("startup")
async def startup():
    init_db()


@app.get("/api/trade-data")
async def get_trade_data():
    """trade.html이 기대하는 완전한 JSON 구조 반환"""
    db_mtime = os.path.getmtime(DB_PATH) if os.path.exists(DB_PATH) else 0
    if _cache["data"] is None or db_mtime > _cache["mtime"]:
        _cache["data"] = build_full_json()
        _cache["mtime"] = db_mtime
    return JSONResponse(content=_cache["data"])


@app.get("/api/provisional-data")
async def get_provisional_data():
    """잠정치: provisional.html이 기대하는 {품목:{h,d,u,s}} 구조 반환.
    정적 /provisional_data.json 과 semantic 동치 (프론트는 이걸 먼저 시도)."""
    db_mtime = os.path.getmtime(DB_PATH) if os.path.exists(DB_PATH) else 0
    if _prov_cache["data"] is None or db_mtime > _prov_cache["mtime"]:
        _prov_cache["data"] = build_provisional_json()
        _prov_cache["mtime"] = db_mtime
    return JSONResponse(content=_prov_cache["data"])


@app.get("/api/health")
async def health():
    return {"status": "ok", "db_exists": os.path.exists(DB_PATH)}


@app.get("/")
async def index():
    return FileResponse(os.path.join(BASE_DIR, "trade.html"))


@app.get("/trade.html")
async def trade_page():
    return FileResponse(os.path.join(BASE_DIR, "trade.html"))


@app.get("/provisional.html")
async def provisional_page():
    return FileResponse(os.path.join(BASE_DIR, "provisional.html"))


@app.get("/provisional_data.json")
async def provisional_data():
    return FileResponse(os.path.join(BASE_DIR, "provisional_data.json"),
                        media_type="application/json")


@app.get("/trade_data_v2.json")
async def trade_data_json():
    """확정치 전체 JSON 정적 서빙 — trade.html의 폴백 2단.
    /api/trade-data가 메모리 부족(53MB 인메모리 직렬화)으로 502일 때
    이 라우트가 없으면 DEMO 임베드로 떨어져 최신 total이 틀리게 보임."""
    return FileResponse(os.path.join(BASE_DIR, "trade_data_v2.json"),
                        media_type="application/json")


@app.get("/business_days.json")
async def business_days():
    return FileResponse(os.path.join(BASE_DIR, "business_days.json"),
                        media_type="application/json")


@app.get("/confirmed_companies.json")
async def confirmed_companies():
    return FileResponse(os.path.join(BASE_DIR, "confirmed_companies.json"),
                        media_type="application/json")


@app.get("/static/{name}")
async def static_file(name: str):
    """국가 메타·세계 GeoJSON 등 정적 자산 (지도 시각화용)."""
    if "/" in name or ".." in name:
        return JSONResponse({"error": "invalid"}, status_code=400)
    path = os.path.join(BASE_DIR, "static", name)
    if not os.path.exists(path):
        return JSONResponse({"error": "not found"}, status_code=404)
    media = "application/json" if name.endswith(".json") else None
    return FileResponse(path, media_type=media)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server.main:app", host="0.0.0.0", port=port, reload=True)
