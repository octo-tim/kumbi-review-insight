"""
로그인·사용자 관리 (Postgres 버전).

- bcrypt로 비밀번호 해싱
- Streamlit session_state로 로그인 상태 유지
- 역할: admin | manager | viewer
"""
from __future__ import annotations

import os

import bcrypt
import pandas as pd
import psycopg
import streamlit as st

from db import get_conn, get_engine

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
    사용자가 0명이면 ADMIN_EMAIL/ADMIN_PASSWORD (또는 기본값)로 admin 1명 생성.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM users")
            row = cur.fetchone()
    if row["n"] > 0:
        return

    email = os.getenv("ADMIN_EMAIL", "").strip().lower() or "admin@example.com"
    password = os.getenv("ADMIN_PASSWORD", "") or "changeme!"

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (email, name, password_hash, role) "
                    "VALUES (%s, %s, %s, 'admin')",
                    (email, "Admin", hash_password(password)),
                )
    except psycopg.errors.UniqueViolation:
        pass


def login(email: str, password: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, email, name, role, password_hash, status "
                "FROM users WHERE email=%s",
                (email.strip().lower(),),
            )
            row = cur.fetchone()
    if not row or row["status"] != "active":
        return None
    if not verify_password(password, row["password_hash"]):
        return None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET last_login_at=now() WHERE id=%s", (row["id"],))

    return {
        "id": row["id"],
        "email": row["email"],
        "name": row["name"],
        "role": row["role"],
    }


def list_users() -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT id, email, name, role, status, created_at, last_login_at "
        "FROM users ORDER BY created_at DESC",
        get_engine(),
    )


def create_user(email: str, name: str, password: str, role: str) -> bool:
    if role not in ROLES:
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (email, name, password_hash, role) "
                    "VALUES (%s, %s, %s, %s)",
                    (email.strip().lower(), name.strip(), hash_password(password), role),
                )
        return True
    except psycopg.errors.UniqueViolation:
        return False
    except psycopg.Error:
        return False


def set_user_status(user_id: int, status: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET status=%s WHERE id=%s", (status, user_id))


def reset_password(user_id: int, new_password: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET password_hash=%s WHERE id=%s",
                (hash_password(new_password), user_id),
            )


# ──────────────────────────────────────────────────────────────
# Streamlit 통합
# ──────────────────────────────────────────────────────────────

def current_user() -> dict | None:
    return st.session_state.get("user")


def require_login() -> dict:
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

    st.caption(
        "초기 관리자 계정은 배포 시 ADMIN_EMAIL / ADMIN_PASSWORD 환경변수로 지정합니다."
    )
    st.stop()


def logout():
    st.session_state.pop("user", None)


def can_edit(user: dict) -> bool:
    return user.get("role") in {"admin", "manager"}


def is_admin(user: dict) -> bool:
    return user.get("role") == "admin"
