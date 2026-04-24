"""
꿈비그룹 통합 리뷰 인사이트 MVP — Streamlit UI.

비즈니스 로직은 classifier.py, 저장은 db.py, 인증은 auth.py에 분리.
이 파일은 UI 조립만 담당합니다.
"""
from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import auth
import db
from classifier import (
    REQUIRED_COLUMNS,
    deduplicate,
    enrich_reviews,
    keyword_count,
    normalize_columns,
)

APP_TITLE = "꿈비그룹 통합 리뷰 인사이트 MVP"

st.set_page_config(page_title=APP_TITLE, layout="wide")

# DB 초기화 + 부트스트랩 관리자 생성
db.init_db()
auth.ensure_bootstrap_admin()


# ──────────────────────────────────────────────────────────────
# 공통 뷰
# ──────────────────────────────────────────────────────────────

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="reviews")
    return output.getvalue()


def sidebar_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    st.sidebar.header("필터")
    date_min = pd.to_datetime(df["review_date"], errors="coerce").min()
    date_max = pd.to_datetime(df["review_date"], errors="coerce").max()
    if pd.isna(date_min):
        date_min = pd.Timestamp.today() - pd.Timedelta(days=30)
    if pd.isna(date_max):
        date_max = pd.Timestamp.today()
    date_range = st.sidebar.date_input("기간", value=(date_min.date(), date_max.date()))

    result = df.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        result = result[
            (pd.to_datetime(result["review_date"]) >= start)
            & (pd.to_datetime(result["review_date"]) <= end)
        ]

    for label, col in [
        ("브랜드", "brand_name"),
        ("상품군", "category_name"),
        ("채널", "channel"),
        ("감성", "sentiment"),
        ("위험도", "risk_level"),
        ("처리상태", "status"),
        ("이슈유형", "issue_type"),
    ]:
        options = ["전체"] + sorted(
            [x for x in result[col].dropna().astype(str).unique() if x]
        )
        selected = st.sidebar.multiselect(label, options, default=["전체"])
        if selected and "전체" not in selected:
            result = result[result[col].astype(str).isin(selected)]

    keyword = st.sidebar.text_input("리뷰 키워드 검색")
    if keyword:
        result = result[
            result["review_text"].astype(str).str.contains(keyword, case=False, na=False)
        ]

    min_r, max_r = st.sidebar.slider("평점 범위", 0.0, 5.0, (0.0, 5.0), 0.5)
    rating_num = pd.to_numeric(result["rating"], errors="coerce")
    result = result[rating_num.fillna(-1).between(min_r, max_r)]

    return result


def render_dashboard(df: pd.DataFrame):
    st.subheader("그룹 홈 대시보드")
    if df.empty:
        st.info("아직 등록된 리뷰가 없습니다. 좌측 메뉴의 ‘리뷰 업로드’에서 샘플 CSV 또는 실제 리뷰 파일을 업로드해 주십시오.")
        return

    total_reviews = len(df)
    valid_rating = pd.to_numeric(df["rating"], errors="coerce").dropna()
    avg_rating = valid_rating.mean() if len(valid_rating) else 0.0
    negative_count = int((df["sentiment"] == "부정").sum())
    risk_count = int((df["risk_level"].isin(["높음", "중간"])).sum())
    recent_7 = int(
        (pd.to_datetime(df["review_date"]) >= (pd.Timestamp.today() - pd.Timedelta(days=7))).sum()
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("전체 리뷰", f"{total_reviews:,}건")
    c2.metric("평균 평점", f"{avg_rating:.2f}")
    c3.metric("부정 리뷰", f"{negative_count:,}건")
    c4.metric("대응 필요", f"{risk_count:,}건")
    c5.metric("최근 7일", f"{recent_7:,}건")

    left, right = st.columns(2)
    daily = (
        df.groupby(pd.to_datetime(df["review_date"]).dt.date)
        .size()
        .reset_index(name="리뷰 수")
    )
    daily.columns = ["작성일", "리뷰 수"]
    with left:
        st.plotly_chart(px.line(daily, x="작성일", y="리뷰 수", title="일자별 리뷰 수"), use_container_width=True)
    brand = df.groupby("brand_name").agg(
        리뷰수=("review_text", "count"),
        평균평점=("rating", "mean"),
    ).reset_index()
    with right:
        st.plotly_chart(px.bar(brand, x="brand_name", y="리뷰수", title="브랜드별 리뷰 수"), use_container_width=True)

    left, right = st.columns(2)
    channel = df.groupby("channel").agg(
        평균평점=("rating", "mean"),
        리뷰수=("review_text", "count"),
    ).reset_index()
    with left:
        st.plotly_chart(px.bar(channel, x="channel", y="평균평점", title="채널별 평균 평점"), use_container_width=True)
    issue = df.groupby("issue_type").size().reset_index(name="건수").sort_values("건수", ascending=False)
    with right:
        st.plotly_chart(px.pie(issue, names="issue_type", values="건수", title="이슈 유형 비중"), use_container_width=True)

    st.markdown("#### 부정 키워드 TOP 10")
    neg_kw = keyword_count(df, "negative_keywords").head(10)
    st.dataframe(neg_kw, use_container_width=True, hide_index=True)


def render_upload(user: dict):
    st.subheader("리뷰 엑셀/CSV 업로드")
    st.caption(f"필수 컬럼: {', '.join(REQUIRED_COLUMNS)}")

    uploaded = st.file_uploader("리뷰 파일 업로드", type=["csv", "xlsx"])
    if not uploaded:
        sample_path = Path("sample_reviews.csv")
        if sample_path.exists():
            st.download_button(
                "샘플 CSV 다운로드",
                sample_path.read_bytes(),
                file_name="sample_reviews.csv",
                mime="text/csv",
            )
        return

    raw = _read_uploaded(uploaded)
    raw = normalize_columns(raw)
    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        st.error(f"필수 컬럼이 누락되었습니다: {', '.join(missing)}")
        st.dataframe(raw.head(20), use_container_width=True)
        return

    kw = db.load_keyword_dict()
    enriched = enrich_reviews(raw, kw)

    st.markdown("#### 업로드 미리보기 (상위 20행)")
    st.dataframe(enriched.head(20), use_container_width=True)

    st.caption(f"총 {len(enriched):,}건, 중 평점 누락 {int(enriched['rating'].isna().sum()):,}건, 작성일 누락 {int(enriched['review_date_was_missing'].sum()):,}건")

    if st.button("검증 완료 후 저장", type="primary"):
        existing = db.existing_dedup_keys()
        inserted_df, duplicated_df = deduplicate(existing, enriched)
        inserted_count, skipped_count = db.insert_reviews(inserted_df)
        db.log_upload(
            user_id=user["id"],
            file_name=uploaded.name,
            total=len(enriched),
            success=inserted_count,
            duplicate=len(duplicated_df) + skipped_count,
            failed=0,
        )
        st.success(
            f"신규 {inserted_count:,}건 저장, 중복 {len(duplicated_df) + skipped_count:,}건 제외"
        )


def _read_uploaded(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        try:
            return pd.read_csv(uploaded_file, encoding="utf-8-sig")
        except UnicodeDecodeError:
            uploaded_file.seek(0)
            return pd.read_csv(uploaded_file, encoding="cp949")
    return pd.read_excel(uploaded_file)


def render_reviews(df: pd.DataFrame, user: dict):
    st.subheader("리뷰 통합 관리")
    if df.empty:
        st.info("표시할 리뷰가 없습니다.")
        return

    view_cols = [
        "id", "review_date", "brand_name", "category_name", "product_name", "channel",
        "rating", "sentiment", "risk_level", "issue_type", "status", "assignee",
        "review_text", "negative_keywords", "positive_keywords", "memo",
    ]
    available = [c for c in view_cols if c in df.columns]
    show = df[available].sort_values("review_date", ascending=False)

    if auth.can_edit(user):
        edited = st.data_editor(
            show,
            use_container_width=True,
            hide_index=True,
            column_config={
                "status": st.column_config.SelectboxColumn(
                    "status", options=["미확인", "처리중", "완료", "보류"]
                ),
                "risk_level": st.column_config.SelectboxColumn(
                    "risk_level", options=["높음", "중간", "낮음"]
                ),
                "review_text": st.column_config.TextColumn(width="large"),
                "memo": st.column_config.TextColumn(width="medium"),
                "id": st.column_config.NumberColumn("id", disabled=True),
            },
            disabled=[c for c in available if c not in {"status", "risk_level", "assignee", "memo"}],
            key="reviews_editor",
        )
        if st.button("변경사항 저장"):
            _persist_review_edits(show, edited)
            st.success("저장되었습니다. 새로고침하면 반영됩니다.")
    else:
        st.dataframe(show, use_container_width=True, hide_index=True)

    st.download_button(
        "현재 목록 엑셀 다운로드",
        data=to_excel_bytes(show.drop(columns=["id"], errors="ignore")),
        file_name=f"kumbi_reviews_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _persist_review_edits(original: pd.DataFrame, edited: pd.DataFrame):
    # id를 기준으로 변경 감지
    if "id" not in original.columns or "id" not in edited.columns:
        return
    merged = original.merge(edited, on="id", suffixes=("_o", "_e"))
    for _, row in merged.iterrows():
        updates = {}
        for f in ["status", "risk_level", "assignee", "memo"]:
            o = row.get(f"{f}_o")
            e = row.get(f"{f}_e")
            if pd.isna(o):
                o = ""
            if pd.isna(e):
                e = ""
            if str(o) != str(e):
                updates[f] = e
        if updates:
            db.update_review_fields(int(row["id"]), **updates)


def render_negative(df: pd.DataFrame):
    st.subheader("부정 리뷰 관리")
    negative = df[
        (df["sentiment"] == "부정") | (df["risk_level"].isin(["높음", "중간"]))
    ].copy()
    if negative.empty:
        st.success("현재 조건에 해당하는 부정 리뷰가 없습니다.")
        return

    by_issue = negative.groupby(["issue_type", "risk_level"]).size().reset_index(name="건수")
    st.plotly_chart(
        px.bar(by_issue, x="issue_type", y="건수", color="risk_level", title="이슈 유형별 위험도"),
        use_container_width=True,
    )

    # risk_level 정렬 (높음→중간→낮음)
    risk_order = {"높음": 0, "중간": 1, "낮음": 2}
    negative["_rank"] = negative["risk_level"].map(risk_order).fillna(3)

    st.dataframe(
        negative[[
            "review_date", "brand_name", "product_name", "channel", "rating",
            "risk_level", "issue_type", "review_text", "negative_keywords",
            "status", "assignee", "memo",
        ]].sort_values(["_rank" if False else "risk_level", "review_date"], ascending=[True, False])
         .drop(columns=["_rank"], errors="ignore"),
        use_container_width=True,
        hide_index=True,
    )


def render_keyword(df: pd.DataFrame):
    st.subheader("키워드 분석")
    if df.empty:
        st.info("분석할 리뷰가 없습니다.")
        return

    pos = keyword_count(df, "positive_keywords").head(20)
    neg = keyword_count(df, "negative_keywords").head(20)

    left, right = st.columns(2)
    with left:
        st.markdown("#### 긍정 키워드")
        st.dataframe(pos, use_container_width=True, hide_index=True)
        if not pos.empty:
            st.plotly_chart(
                px.bar(pos.head(10), x="keyword", y="count", title="긍정 키워드 TOP 10"),
                use_container_width=True,
            )
    with right:
        st.markdown("#### 부정 키워드")
        st.dataframe(neg, use_container_width=True, hide_index=True)
        if not neg.empty:
            st.plotly_chart(
                px.bar(neg.head(10), x="keyword", y="count", title="부정 키워드 TOP 10"),
                use_container_width=True,
            )


def render_product_analysis(df: pd.DataFrame):
    st.subheader("브랜드·상품별 분석")
    if df.empty:
        st.info("분석할 리뷰가 없습니다.")
        return
    summary = df.groupby(["brand_name", "category_name", "product_name"]).agg(
        리뷰수=("review_text", "count"),
        평균평점=("rating", "mean"),
        부정리뷰수=("sentiment", lambda s: int((s == "부정").sum())),
        고위험수=("risk_level", lambda s: int((s == "높음").sum())),
    ).reset_index()
    summary["부정리뷰율"] = (summary["부정리뷰수"] / summary["리뷰수"] * 100).round(1)
    summary["평균평점"] = summary["평균평점"].round(2)
    st.dataframe(
        summary.sort_values(["부정리뷰율", "리뷰수"], ascending=[False, False]),
        use_container_width=True,
        hide_index=True,
    )
    st.plotly_chart(
        px.scatter(
            summary, x="리뷰수", y="평균평점", size="고위험수", color="brand_name",
            hover_data=["product_name", "부정리뷰율"], title="상품별 리뷰 수·평점·위험도",
        ),
        use_container_width=True,
    )


# ──────────────────────────────────────────────────────────────
# 관리자 메뉴
# ──────────────────────────────────────────────────────────────

def render_admin_users(user: dict):
    st.subheader("사용자 관리")
    st.dataframe(auth.list_users(), use_container_width=True, hide_index=True)

    st.markdown("#### 사용자 추가")
    with st.form("add_user_form", clear_on_submit=True):
        email = st.text_input("이메일")
        name = st.text_input("이름")
        password = st.text_input("임시 비밀번호", type="password")
        role = st.selectbox("역할", auth.ROLES, index=2)
        submitted = st.form_submit_button("추가")
    if submitted:
        if not email or not password:
            st.error("이메일과 비밀번호를 입력해 주십시오.")
        elif auth.create_user(email, name or email, password, role):
            st.success(f"사용자 {email} 추가됨")
        else:
            st.error("이미 존재하는 이메일이거나 생성에 실패했습니다.")


def render_admin_keywords():
    st.subheader("키워드 사전 관리")
    st.caption("감성 분류·위험도에 사용됩니다. 변경 후 새로 업로드된 리뷰부터 반영됩니다.")

    kw_df = db.list_keywords()
    st.dataframe(kw_df, use_container_width=True, hide_index=True)

    st.markdown("#### 키워드 추가")
    with st.form("add_kw_form", clear_on_submit=True):
        keyword = st.text_input("키워드")
        sentiment_type = st.selectbox("구분", ["negative", "positive", "strong_negative"])
        submitted = st.form_submit_button("추가")
    if submitted:
        if keyword.strip() and db.add_keyword(keyword.strip(), sentiment_type):
            st.success("추가되었습니다.")
        else:
            st.error("이미 존재하거나 입력이 잘못되었습니다.")

    st.markdown("#### 이슈 유형 룰")
    rules = db.list_issue_rules()
    st.dataframe(rules, use_container_width=True, hide_index=True)

    with st.form("add_rule_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        issue_type = c1.text_input("이슈 유형", placeholder="예: 제품 품질")
        keyword = c2.text_input("키워드")
        priority = c3.number_input("우선순위(작을수록 먼저)", value=50, step=1)
        submitted = st.form_submit_button("추가")
    if submitted:
        if issue_type.strip() and keyword.strip() and db.add_issue_rule(issue_type.strip(), keyword.strip(), int(priority)):
            st.success("추가되었습니다.")
        else:
            st.error("이미 존재하거나 입력이 잘못되었습니다.")


def render_admin_uploads():
    st.subheader("업로드 이력")
    df = db.recent_upload_logs(50)
    st.dataframe(df, use_container_width=True, hide_index=True)


def render_admin_reset(user: dict):
    st.subheader("데이터 초기화")
    st.warning("저장된 리뷰 전체를 삭제합니다. 복구 불가능합니다.")
    confirm = st.text_input("확인을 위해 '삭제'를 입력해 주십시오.")
    if st.button("전체 리뷰 데이터 삭제", type="primary"):
        if confirm == "삭제":
            db.truncate_reviews()
            st.success("초기화되었습니다.")
        else:
            st.error("확인 문구가 일치하지 않습니다.")


# ──────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────

def main():
    user = auth.require_login()

    st.title(APP_TITLE)
    st.caption(f"로그인: {user['name']} ({user['email']}, 역할: {user['role']})")

    with st.sidebar:
        if st.button("로그아웃"):
            auth.logout()
            st.rerun()

    menu_items = ["홈 대시보드", "리뷰 업로드", "리뷰 통합 관리", "부정 리뷰 관리", "키워드 분석", "브랜드·상품별 분석"]
    if auth.is_admin(user):
        menu_items += ["관리자: 사용자", "관리자: 키워드 사전", "관리자: 업로드 이력", "관리자: 데이터 초기화"]
    menu = st.sidebar.radio("메뉴", menu_items)

    if menu == "리뷰 업로드":
        render_upload(user)
        return
    if menu == "관리자: 사용자":
        render_admin_users(user)
        return
    if menu == "관리자: 키워드 사전":
        render_admin_keywords()
        return
    if menu == "관리자: 업로드 이력":
        render_admin_uploads()
        return
    if menu == "관리자: 데이터 초기화":
        render_admin_reset(user)
        return

    master = db.load_reviews()
    filtered = sidebar_filter(master)

    if menu == "홈 대시보드":
        render_dashboard(filtered)
    elif menu == "리뷰 통합 관리":
        render_reviews(filtered, user)
    elif menu == "부정 리뷰 관리":
        render_negative(filtered)
    elif menu == "키워드 분석":
        render_keyword(filtered)
    elif menu == "브랜드·상품별 분석":
        render_product_analysis(filtered)


if __name__ == "__main__":
    main()
