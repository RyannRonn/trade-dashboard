"""FastAPI 서버: trade.html에 데이터 제공"""
import os
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .config import BASE_DIR, DB_PATH
from .builder import build_full_json
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


@app.get("/api/health")
async def health():
    return {"status": "ok", "db_exists": os.path.exists(DB_PATH)}


@app.get("/")
async def index():
    return FileResponse(os.path.join(BASE_DIR, "trade.html"))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server.main:app", host="0.0.0.0", port=port, reload=True)
