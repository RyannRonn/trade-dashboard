"""SQLite 스키마 정의 및 초기화"""
import sqlite3
from .config import DB_PATH

SCHEMA_SQL = """
-- 메타데이터
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- HS코드 이름 사전 (2/4/6/10자리 통합)
CREATE TABLE IF NOT EXISTS hs_names (
    hs_code TEXT PRIMARY KEY,
    name    TEXT NOT NULL,
    digits  INTEGER NOT NULL
);

-- 국가 코드
CREATE TABLE IF NOT EXISTS countries (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

-- 지역 코드
CREATE TABLE IF NOT EXISTS regions (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

-- 품목 정의
CREATE TABLE IF NOT EXISTS items (
    hs_code    TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    is_main    INTEGER DEFAULT 0,
    sort_order INTEGER DEFAULT 0
);

-- 품목별 수집대상 국가
CREATE TABLE IF NOT EXISTS item_countries (
    hs_code      TEXT NOT NULL,
    country_code TEXT NOT NULL,
    PRIMARY KEY (hs_code, country_code)
);

-- 세부항목 정의
CREATE TABLE IF NOT EXISTS sub_items (
    hs_code  TEXT NOT NULL,
    sub_code TEXT NOT NULL,
    name     TEXT NOT NULL,
    PRIMARY KEY (hs_code, sub_code)
);

-- 기업 정의
CREATE TABLE IF NOT EXISTS companies (
    hs_code     TEXT NOT NULL,
    company_key TEXT NOT NULL,
    name        TEXT NOT NULL,
    PRIMARY KEY (hs_code, company_key)
);

-- 기업 사업장
CREATE TABLE IF NOT EXISTS company_locations (
    hs_code      TEXT NOT NULL,
    company_key  TEXT NOT NULL,
    location_key TEXT NOT NULL,
    name         TEXT NOT NULL,
    PRIMARY KEY (hs_code, company_key, location_key)
);

-- 시계열 데이터 (통합)
CREATE TABLE IF NOT EXISTS trade_data (
    data_type   TEXT NOT NULL,
    hs_code     TEXT NOT NULL DEFAULT '',
    sub_code    TEXT NOT NULL DEFAULT '',
    entity_code TEXT NOT NULL DEFAULT '',
    ym          TEXT NOT NULL,
    exp_usd     INTEGER DEFAULT 0,
    imp_usd     INTEGER DEFAULT 0,
    wgt         INTEGER DEFAULT 0,
    PRIMARY KEY (data_type, hs_code, sub_code, entity_code, ym)
);

CREATE INDEX IF NOT EXISTS idx_trade_ym ON trade_data(ym);
CREATE INDEX IF NOT EXISTS idx_trade_type_hs ON trade_data(data_type, hs_code);
CREATE INDEX IF NOT EXISTS idx_trade_type_hs_sub ON trade_data(data_type, hs_code, sub_code);
CREATE INDEX IF NOT EXISTS idx_hs_names_digits ON hs_names(digits);

-- 수집 이력
CREATE TABLE IF NOT EXISTS collection_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    script_name  TEXT NOT NULL,
    hs_code      TEXT NOT NULL DEFAULT '',
    ym_start     TEXT NOT NULL,
    ym_end       TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    row_count    INTEGER DEFAULT 0
);
"""


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # 읽기 성능 최적화
    conn.execute("PRAGMA cache_size=-20000")       # 20MB 페이지 캐시
    conn.execute("PRAGMA mmap_size=50000000")       # 50MB mmap (DB 전체 메모리맵)
    conn.execute("PRAGMA temp_store=MEMORY")        # 임시 테이블 메모리 사용
    return conn


def init_db():
    """테이블 생성 (이미 있으면 무시)"""
    conn = get_connection()
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
