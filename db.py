"""
SQLite 데이터 계층.

CSV 기반 원본 MVP 대비 변경:
- reviews 테이블에 UNIQUE(dedup_key) → INSERT OR IGNORE로 중복 안전하게 처리
- 트랜잭션 기반 일괄 insert → 1만 행 업로드 안정성 확보
- users 테이블 추가 (로그인·권한)
- keyword_dictionary, issue_rules 테이블 추가 (RFP 6.6 "관리자가 키워드 사전 추가·수정")
- upload_logs 테이블 추가 (RFP 6.2 "업로드 성공/실패/중복 건수 기록")
- 모든 I/O가 한 곳에 모여 있어 Node/Express 포팅 시 스키마를 그대로 옮기기 쉬움
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd

from classifier import (
    DEFAULT_KEYWORDS,
    ENRICHED_EXTRA_COLUMNS,
    KeywordDict,
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
)

DB_PATH = Path(os.getenv("DB_PATH", "data/reviews.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Streamlit 세션에서 재사용될 수 있어 thread-safe 하게 같은 connection 재개방
_connection: sqlite3.Connection | None = None


def get_conn() -> sqlite3.Connection:
    global _connection
    if _connection is None:
        _connection = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
        _connection.row_factory = sqlite3.Row
        _connection.execute("PRAGMA journal_mode=WAL")
        _connection.execute("PRAGMA foreign_keys=ON")
    return _connection


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    conn = get_conn()
    try:
        conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ──────────────────────────────────────────────────────────────
# 스키마
# ──────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_login_at TEXT
);

CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_name TEXT,
    category_name TEXT,
    product_name TEXT,
    channel TEXT,
    rating REAL,
    review_text TEXT,
    review_date TEXT,
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
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    dedup_key TEXT UNIQUE NOT NULL,
    review_date_was_missing INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_reviews_brand ON reviews(brand_name);
CREATE INDEX IF NOT EXISTS idx_reviews_category ON reviews(category_name);
CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_name);
CREATE INDEX IF NOT EXISTS idx_reviews_channel ON reviews(channel);
CREATE INDEX IF NOT EXISTS idx_reviews_sentiment ON reviews(sentiment);
CREATE INDEX IF NOT EXISTS idx_reviews_risk ON reviews(risk_level);
CREATE INDEX IF NOT EXISTS idx_reviews_status ON reviews(status);
CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(review_date);

CREATE TABLE IF NOT EXISTS upload_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    file_name TEXT,
    total_rows INTEGER,
    success_rows INTEGER,
    duplicate_rows INTEGER,
    failed_rows INTEGER,
    uploaded_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS keyword_dictionary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT NOT NULL,
    sentiment_type TEXT NOT NULL,   -- 'negative' | 'positive' | 'strong_negative'
    category_scope TEXT DEFAULT '',  -- '' = 전체, 아니면 특정 상품군
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(keyword, sentiment_type, category_scope)
);

CREATE TABLE IF NOT EXISTS issue_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_type TEXT NOT NULL,
    keyword TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    UNIQUE(issue_type, keyword)
);
"""


def init_db():
    conn = get_conn()
    conn.executescript(SCHEMA)
    _seed_default_keywords()


def _seed_default_keywords():
    """DB가 비어 있으면 DEFAULT_KEYWORDS 내용으로 1회 시드."""
    conn = get_conn()
    row = conn.execute("SELECT COUNT(*) AS n FROM keyword_dictionary").fetchone()
    if row["n"] > 0:
        return
    with transaction() as c:
        for kw in DEFAULT_KEYWORDS.negative:
            c.execute(
                "INSERT OR IGNORE INTO keyword_dictionary(keyword, sentiment_type) VALUES (?, 'negative')",
                (kw,),
            )
        for kw in DEFAULT_KEYWORDS.positive:
            c.execute(
                "INSERT OR IGNORE INTO keyword_dictionary(keyword, sentiment_type) VALUES (?, 'positive')",
                (kw,),
            )
        for kw in DEFAULT_KEYWORDS.strong_negative:
            c.execute(
                "INSERT OR IGNORE INTO keyword_dictionary(keyword, sentiment_type) VALUES (?, 'strong_negative')",
                (kw,),
            )
        # 선언 순서 priority로 이관
        for priority, (issue_type, kws) in enumerate(DEFAULT_KEYWORDS.issue_rules.items()):
            for kw in kws:
                c.execute(
                    "INSERT OR IGNORE INTO issue_rules(issue_type, keyword, priority) VALUES (?, ?, ?)",
                    (issue_type, kw, priority),
                )


# ──────────────────────────────────────────────────────────────
# 키워드 사전 로드
# ──────────────────────────────────────────────────────────────

def load_keyword_dict() -> KeywordDict:
    """DB에서 현재 활성 키워드 사전 불러오기."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT keyword, sentiment_type FROM keyword_dictionary WHERE status='active'"
    ).fetchall()
    negative = [r["keyword"] for r in rows if r["sentiment_type"] == "negative"]
    positive = [r["keyword"] for r in rows if r["sentiment_type"] == "positive"]
    strong = [r["keyword"] for r in rows if r["sentiment_type"] == "strong_negative"]

    issue_rows = conn.execute(
        "SELECT issue_type, keyword, priority FROM issue_rules WHERE status='active' ORDER BY priority, id"
    ).fetchall()
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


def _to_db_value(v):
    """pandas 타입을 sqlite3가 받을 수 있는 타입으로 변환."""
    if pd.isna(v):
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, bool):
        return 1 if v else 0
    return v


def insert_reviews(df: pd.DataFrame) -> tuple[int, int]:
    """
    enriched DataFrame을 reviews 테이블에 삽입.

    UNIQUE(dedup_key) 위반은 INSERT OR IGNORE로 조용히 스킵.

    반환: (inserted_count, skipped_duplicate_count)
    """
    if df.empty:
        return 0, 0

    cols = [c for c in ALL_REVIEW_COLUMNS if c in df.columns]
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(cols)
    sql = f"INSERT OR IGNORE INTO reviews ({col_list}) VALUES ({placeholders})"

    rows = [tuple(_to_db_value(v) for v in row) for row in df[cols].itertuples(index=False)]

    conn = get_conn()
    before = conn.execute("SELECT COUNT(*) AS n FROM reviews").fetchone()["n"]
    with transaction() as c:
        c.executemany(sql, rows)
    after = conn.execute("SELECT COUNT(*) AS n FROM reviews").fetchone()["n"]

    inserted = after - before
    skipped = len(rows) - inserted
    return inserted, skipped


def existing_dedup_keys() -> set[str]:
    conn = get_conn()
    return {r["dedup_key"] for r in conn.execute("SELECT dedup_key FROM reviews")}


def load_reviews() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM reviews", conn)
    if "review_date" in df.columns:
        df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    return df


def update_review_fields(review_id: int, **fields):
    """상태/담당자/메모 갱신."""
    allowed = {"status", "assignee", "memo", "risk_level"}
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return
    sets = ", ".join(f"{k}=?" for k in clean)
    values = list(clean.values()) + [review_id]
    conn = get_conn()
    conn.execute(f"UPDATE reviews SET {sets} WHERE id=?", values)


def truncate_reviews():
    conn = get_conn()
    conn.execute("DELETE FROM reviews")


def log_upload(user_id: int | None, file_name: str, total: int, success: int, duplicate: int, failed: int):
    conn = get_conn()
    conn.execute(
        """INSERT INTO upload_logs (user_id, file_name, total_rows, success_rows, duplicate_rows, failed_rows)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (user_id, file_name, total, success, duplicate, failed),
    )


def recent_upload_logs(limit: int = 20) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(
        """SELECT l.*, u.email AS user_email FROM upload_logs l
           LEFT JOIN users u ON u.id = l.user_id
           ORDER BY l.uploaded_at DESC LIMIT ?""",
        conn,
        params=(limit,),
    )


# ──────────────────────────────────────────────────────────────
# 키워드 사전 CRUD
# ──────────────────────────────────────────────────────────────

def list_keywords() -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(
        "SELECT id, keyword, sentiment_type, category_scope, status, created_at FROM keyword_dictionary ORDER BY sentiment_type, keyword",
        conn,
    )


def add_keyword(keyword: str, sentiment_type: str, category_scope: str = "") -> bool:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO keyword_dictionary(keyword, sentiment_type, category_scope) VALUES (?, ?, ?)",
            (keyword.strip(), sentiment_type, category_scope),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def set_keyword_status(keyword_id: int, status: str):
    conn = get_conn()
    conn.execute("UPDATE keyword_dictionary SET status=? WHERE id=?", (status, keyword_id))


def delete_keyword(keyword_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM keyword_dictionary WHERE id=?", (keyword_id,))


def list_issue_rules() -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(
        "SELECT id, issue_type, keyword, priority, status FROM issue_rules ORDER BY priority, issue_type, keyword",
        conn,
    )


def add_issue_rule(issue_type: str, keyword: str, priority: int = 99) -> bool:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO issue_rules(issue_type, keyword, priority) VALUES (?, ?, ?)",
            (issue_type.strip(), keyword.strip(), priority),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def delete_issue_rule(rule_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM issue_rules WHERE id=?", (rule_id,))
