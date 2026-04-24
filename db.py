"""
PostgreSQL 데이터 계층 (psycopg 3).

SQLite 버전에서 Postgres로 전환하면서 바뀐 핵심:
- INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
- datetime('now') → now() (Postgres 함수)
- AUTOINCREMENT → GENERATED ALWAYS AS IDENTITY
- placeholder: ? → %s
- WAL 모드 불필요 (Postgres는 기본 MVCC)
- ConnectionPool로 동시 20~50명 요건 대응 (RFP 12장)

환경변수:
- DATABASE_URL: Railway Postgres 플러그인 연결 시 자동 주입.
  postgres://user:pass@host:port/dbname 형식.
- Railway 서비스끼리 같은 프로젝트라면 Postgres 플러그인을
  'Variables' 탭에서 DATABASE_URL로 참조 추가해 주면 됩니다.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from classifier import (
    DEFAULT_KEYWORDS,
    ENRICHED_EXTRA_COLUMNS,
    KeywordDict,
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
)


def _get_database_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL 환경변수가 설정되지 않았습니다. "
            "Railway Postgres 플러그인을 Variables에서 참조하거나, "
            "로컬 테스트 시 export DATABASE_URL='postgresql://...'"
        )
    # Railway는 가끔 postgres:// 형식을 주는데, psycopg 3는 postgresql://를 선호
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url


# ConnectionPool: Streamlit 환경에서 각 rerun마다 연결을 재생성하지 않도록.
_pool: ConnectionPool | None = None
# SQLAlchemy 엔진: pd.read_sql_query 전용 (psycopg 3 DBAPI 객체는 pandas가 경고 띄움)
_engine: Engine | None = None


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=_get_database_url(),
            min_size=1,
            max_size=10,
            kwargs={"autocommit": True, "row_factory": dict_row},
            open=True,
        )
    return _pool


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = _get_database_url()
        # SQLAlchemy 2.x는 postgresql:// URL에서 기본적으로 psycopg2를 찾는다.
        # 우리는 psycopg 3만 설치했으므로 driver를 명시.
        if url.startswith("postgresql://"):
            sa_url = "postgresql+psycopg://" + url[len("postgresql://"):]
        else:
            sa_url = url
        _engine = create_engine(sa_url, pool_size=5, max_overflow=5, pool_pre_ping=True)
    return _engine


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


@contextmanager
def transaction() -> Iterator[psycopg.Connection]:
    """autocommit 풀에서 명시적 트랜잭션 블록."""
    pool = get_pool()
    with pool.connection() as conn:
        # psycopg 3의 autocommit=True에서 수동 트랜잭션을 걸려면
        # conn.transaction() 컨텍스트를 씁니다.
        with conn.transaction():
            yield conn


# ──────────────────────────────────────────────────────────────
# 스키마
# ──────────────────────────────────────────────────────────────

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'viewer',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_login_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reviews (
        id BIGSERIAL PRIMARY KEY,
        brand_name TEXT,
        category_name TEXT,
        product_name TEXT,
        channel TEXT,
        rating REAL,
        review_text TEXT,
        review_date DATE,
        option_name TEXT,
        writer TEXT,
        review_url TEXT,
        order_no TEXT,
        image_yn TEXT,
        reply_yn TEXT,
        writer_masked TEXT,
        negative_keywords TEXT,
        positive_keywords TEXT,
        sentiment TEXT,
        risk_level TEXT,
        issue_type TEXT,
        issue_types_all TEXT,
        status TEXT DEFAULT '미확인',
        assignee TEXT,
        memo TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        dedup_key TEXT UNIQUE NOT NULL,
        review_date_was_missing BOOLEAN DEFAULT FALSE
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_reviews_brand ON reviews(brand_name)",
    "CREATE INDEX IF NOT EXISTS idx_reviews_category ON reviews(category_name)",
    "CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_name)",
    "CREATE INDEX IF NOT EXISTS idx_reviews_channel ON reviews(channel)",
    "CREATE INDEX IF NOT EXISTS idx_reviews_sentiment ON reviews(sentiment)",
    "CREATE INDEX IF NOT EXISTS idx_reviews_risk ON reviews(risk_level)",
    "CREATE INDEX IF NOT EXISTS idx_reviews_status ON reviews(status)",
    "CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(review_date)",
    """
    CREATE TABLE IF NOT EXISTS upload_logs (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
        file_name TEXT,
        total_rows INTEGER,
        success_rows INTEGER,
        duplicate_rows INTEGER,
        failed_rows INTEGER,
        uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS keyword_dictionary (
        id BIGSERIAL PRIMARY KEY,
        keyword TEXT NOT NULL,
        sentiment_type TEXT NOT NULL,
        category_scope TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE(keyword, sentiment_type, category_scope)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS issue_rules (
        id BIGSERIAL PRIMARY KEY,
        issue_type TEXT NOT NULL,
        keyword TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'active',
        UNIQUE(issue_type, keyword)
    )
    """,
]


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            for stmt in SCHEMA_STATEMENTS:
                cur.execute(stmt)
    _seed_default_keywords()


def _seed_default_keywords():
    """DB가 비어 있으면 DEFAULT_KEYWORDS 내용으로 1회 시드."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM keyword_dictionary")
            row = cur.fetchone()
            if row["n"] > 0:
                return

    with transaction() as conn:
        with conn.cursor() as cur:
            for kw in DEFAULT_KEYWORDS.negative:
                cur.execute(
                    "INSERT INTO keyword_dictionary(keyword, sentiment_type) "
                    "VALUES (%s, 'negative') ON CONFLICT DO NOTHING",
                    (kw,),
                )
            for kw in DEFAULT_KEYWORDS.positive:
                cur.execute(
                    "INSERT INTO keyword_dictionary(keyword, sentiment_type) "
                    "VALUES (%s, 'positive') ON CONFLICT DO NOTHING",
                    (kw,),
                )
            for kw in DEFAULT_KEYWORDS.strong_negative:
                cur.execute(
                    "INSERT INTO keyword_dictionary(keyword, sentiment_type) "
                    "VALUES (%s, 'strong_negative') ON CONFLICT DO NOTHING",
                    (kw,),
                )
            for priority, (issue_type, kws) in enumerate(DEFAULT_KEYWORDS.issue_rules.items()):
                for kw in kws:
                    cur.execute(
                        "INSERT INTO issue_rules(issue_type, keyword, priority) "
                        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (issue_type, kw, priority),
                    )


# ──────────────────────────────────────────────────────────────
# 키워드 사전 로드
# ──────────────────────────────────────────────────────────────

def load_keyword_dict() -> KeywordDict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT keyword, sentiment_type FROM keyword_dictionary WHERE status='active'"
            )
            rows = cur.fetchall()
            cur.execute(
                "SELECT issue_type, keyword, priority FROM issue_rules "
                "WHERE status='active' ORDER BY priority, id"
            )
            issue_rows = cur.fetchall()

    negative = [r["keyword"] for r in rows if r["sentiment_type"] == "negative"]
    positive = [r["keyword"] for r in rows if r["sentiment_type"] == "positive"]
    strong = [r["keyword"] for r in rows if r["sentiment_type"] == "strong_negative"]

    issue_rules: dict[str, list[str]] = {}
    for r in issue_rows:
        issue_rules.setdefault(r["issue_type"], []).append(r["keyword"])

    return KeywordDict(
        negative=negative or DEFAULT_KEYWORDS.negative,
        positive=positive or DEFAULT_KEYWORDS.positive,
        strong_negative=strong or DEFAULT_KEYWORDS.strong_negative,
        issue_rules=issue_rules or DEFAULT_KEYWORDS.issue_rules,
    )


# ──────────────────────────────────────────────────────────────
# 리뷰 저장·조회
# ──────────────────────────────────────────────────────────────

ALL_REVIEW_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS + ENRICHED_EXTRA_COLUMNS


def _to_db_value(col: str, v):
    """pandas 타입을 psycopg가 받을 수 있는 타입으로 변환."""
    if pd.isna(v):
        return None
    if col == "review_date" and hasattr(v, "strftime"):
        return v.date() if hasattr(v, "date") else v
    if col == "review_date_was_missing":
        return bool(v)
    if isinstance(v, bool):
        return v
    if hasattr(v, "item"):  # numpy 스칼라 → 파이썬 기본형
        return v.item()
    return v


def insert_reviews(df: pd.DataFrame) -> tuple[int, int]:
    """
    enriched DataFrame을 reviews 테이블에 삽입.
    UNIQUE(dedup_key) 위반은 ON CONFLICT DO NOTHING으로 조용히 스킵.

    반환: (inserted_count, skipped_duplicate_count)
    """
    if df.empty:
        return 0, 0

    cols = [c for c in ALL_REVIEW_COLUMNS if c in df.columns]
    placeholders = ",".join(["%s"] * len(cols))
    col_list = ",".join(cols)
    sql = (
        f"INSERT INTO reviews ({col_list}) VALUES ({placeholders}) "
        "ON CONFLICT (dedup_key) DO NOTHING"
    )

    rows = [
        tuple(_to_db_value(col, v) for col, v in zip(cols, row))
        for row in df[cols].itertuples(index=False)
    ]

    total = len(rows)
    inserted = 0
    with transaction() as conn:
        with conn.cursor() as cur:
            # executemany 대신 per-row 실행으로 정확한 inserted 카운트 확보
            # (대량 업로드 시 executemany가 훨씬 빠르지만 rowcount 누적이 드라이버 의존적)
            for r in rows:
                cur.execute(sql, r)
                inserted += cur.rowcount

    return inserted, total - inserted


def existing_dedup_keys() -> set[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT dedup_key FROM reviews")
            return {r["dedup_key"] for r in cur.fetchall()}


def load_reviews() -> pd.DataFrame:
    df = pd.read_sql_query("SELECT * FROM reviews", get_engine())
    if "review_date" in df.columns:
        df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    return df


def update_review_fields(review_id: int, **fields):
    allowed = {"status", "assignee", "memo", "risk_level"}
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return
    sets = ", ".join(f"{k}=%s" for k in clean)
    values = list(clean.values()) + [review_id]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE reviews SET {sets} WHERE id=%s", values)


def truncate_reviews():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE reviews RESTART IDENTITY")


def log_upload(user_id: int | None, file_name: str, total: int, success: int, duplicate: int, failed: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO upload_logs (user_id, file_name, total_rows, success_rows, duplicate_rows, failed_rows)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (user_id, file_name, total, success, duplicate, failed),
            )


def recent_upload_logs(limit: int = 20) -> pd.DataFrame:
    return pd.read_sql_query(
        """SELECT l.*, u.email AS user_email FROM upload_logs l
           LEFT JOIN users u ON u.id = l.user_id
           ORDER BY l.uploaded_at DESC LIMIT %(limit)s""",
        get_engine(),
        params={"limit": limit},
    )


# ──────────────────────────────────────────────────────────────
# 키워드 사전 CRUD
# ──────────────────────────────────────────────────────────────

def list_keywords() -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT id, keyword, sentiment_type, category_scope, status, created_at "
        "FROM keyword_dictionary ORDER BY sentiment_type, keyword",
        get_engine(),
    )


def add_keyword(keyword: str, sentiment_type: str, category_scope: str = "") -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO keyword_dictionary(keyword, sentiment_type, category_scope) "
                    "VALUES (%s, %s, %s)",
                    (keyword.strip(), sentiment_type, category_scope),
                )
        return True
    except psycopg.errors.UniqueViolation:
        return False
    except psycopg.Error:
        return False


def set_keyword_status(keyword_id: int, status: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE keyword_dictionary SET status=%s WHERE id=%s", (status, keyword_id)
            )


def delete_keyword(keyword_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM keyword_dictionary WHERE id=%s", (keyword_id,))


def list_issue_rules() -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT id, issue_type, keyword, priority, status FROM issue_rules "
        "ORDER BY priority, issue_type, keyword",
        get_engine(),
    )


def add_issue_rule(issue_type: str, keyword: str, priority: int = 99) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO issue_rules(issue_type, keyword, priority) "
                    "VALUES (%s, %s, %s)",
                    (issue_type.strip(), keyword.strip(), priority),
                )
        return True
    except psycopg.errors.UniqueViolation:
        return False
    except psycopg.Error:
        return False


def delete_issue_rule(rule_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM issue_rules WHERE id=%s", (rule_id,))
