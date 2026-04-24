"""
로그인·사용자 관리.

- bcrypt로 비밀번호 해싱 (RFP 10: "복호화 불가능한 해시")
- Streamlit session_state로 로그인 상태 유지
- 역할(role)은 단순화: admin | manager | viewer
  향후 RFP 5장의 9개 역할 체계로 확장 가능

초기 관리자 계정:
- 최초 기동 시 ADMIN_EMAIL / ADMIN_PASSWORD 환경변수로 1개 생성
- 이후는 관리자 메뉴에서 추가
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime

import bcrypt
import pandas as pd
import streamlit as st

from db import get_conn

ROLES = ["admin", "manager", "viewer"]


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False


def ensure_bootstrap_admin():
    """
    ADMIN_EMAIL / ADMIN_PASSWORD 환경변수가 있고 사용자가 0명이면
    해당 계정을 admin 역할로 1회 생성.
    """
    conn = get_conn()
    cnt = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    if cnt > 0:
        return

    email = os.getenv("ADMIN_EMAIL", "").strip().lower()
    password = os.getenv("ADMIN_PASSWORD", "")
    if not email or not password:
        # 환경변수 없으면 기본 관리자 생성 (최초 1회, 개발용)
        email = "admin@example.com"
        password = "changeme!"
    try:
        conn.execute(
            "INSERT INTO users (email, name, password_hash, role) VALUES (?, ?, ?, 'admin')",
            (email, "Admin", hash_password(password)),
        )
    except sqlite3.IntegrityError:
        pass


def login(email: str, password: str) -> dict | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT id, email, name, role, password_hash, status FROM users WHERE email=?",
        (email.strip().lower(),),
    ).fetchone()
    if not row or row["status"] != "active":
        return None
    if not verify_password(password, row["password_hash"]):
        return None
    conn.execute(
        "UPDATE users SET last_login_at=? WHERE id=?",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), row["id"]),
    )
    return {"id": row["id"], "email": row["email"], "name": row["name"], "role": row["role"]}


def list_users() -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(
        "SELECT id, email, name, role, status, created_at, last_login_at FROM users ORDER BY created_at DESC",
        conn,
    )


def create_user(email: str, name: str, password: str, role: str) -> bool:
    if role not in ROLES:
        return False
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO users (email, name, password_hash, role) VALUES (?, ?, ?, ?)",
            (email.strip().lower(), name.strip(), hash_password(password), role),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def set_user_status(user_id: int, status: str):
    conn = get_conn()
    conn.execute("UPDATE users SET status=? WHERE id=?", (status, user_id))


def reset_password(user_id: int, new_password: str):
    conn = get_conn()
    conn.execute(
        "UPDATE users SET password_hash=? WHERE id=?",
        (hash_password(new_password), user_id),
    )


# ──────────────────────────────────────────────────────────────
# Streamlit 통합
# ──────────────────────────────────────────────────────────────

def current_user() -> dict | None:
    return st.session_state.get("user")


def require_login() -> dict:
    """
    로그인 폼을 렌더링하고 로그인된 사용자를 반환.
    로그인 안 됐으면 st.stop()으로 렌더링 중단.
    """
    user = current_user()
    if user:
        return user

    st.title("꿈비그룹 리뷰 인사이트 · 로그인")
    st.caption("이메일과 비밀번호로 로그인해 주십시오.")

    with st.form("login_form", clear_on_submit=False):
        email = st.text_input("이메일")
        password = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인", type="primary")

    if submitted:
        u = login(email, password)
        if u:
            st.session_state["user"] = u
            st.rerun()
        else:
            st.error("이메일 또는 비밀번호가 올바르지 않습니다.")

    st.caption("초기 관리자 계정은 배포 시 ADMIN_EMAIL / ADMIN_PASSWORD 환경변수로 지정합니다.")
    st.stop()


def logout():
    st.session_state.pop("user", None)


def can_edit(user: dict) -> bool:
    return user.get("role") in {"admin", "manager"}


def is_admin(user: dict) -> bool:
    return user.get("role") == "admin"
