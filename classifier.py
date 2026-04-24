"""
리뷰 분류 로직.

RFP 9장(분석·분류 로직)의 순수 비즈니스 로직을 모읍니다.
Streamlit에 독립적이므로 단위 테스트가 가능하고, 향후 Node/Express 포팅 시
동등성 확인의 레퍼런스로 활용할 수 있습니다.

원본 MVP 대비 변경 내역:

- dedup_key를 review_url(3순위) → order_no(2순위) → channel+product+date+text_hash(1순위)
  fallback 체인으로 확장 (RFP 8.1).
- rating 누락 시 NaN을 유지하고, 부정 자동 분류에서 제외 (원본 버그: rating=NaN이
  fillna(0)으로 떨어져 모든 무평점 리뷰가 부정으로 분류되던 문제).
- 이슈 유형 분류를 "첫 매치" 방식에서 "hit 수 최다 + 선언순서 tie-break"로 변경.
  동시에 매칭된 모든 카테고리를 issue_types_all에 기록.
- keyword_count의 pandas 2.2+ reset_index 컬럼명 이슈 수정.
- 키워드 사전과 이슈 룰을 KeywordDict 데이터클래스로 분리해 주입 가능하게.
  향후 DB 테이블에서 로드하는 구조로 자연스럽게 확장됩니다.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd

REQUIRED_COLUMNS = [
    "brand_name",
    "category_name",
    "product_name",
    "channel",
    "rating",
    "review_text",
    "review_date",
]

OPTIONAL_COLUMNS = [
    "option_name",
    "writer",
    "review_url",
    "order_no",
    "image_yn",
    "reply_yn",
]

COLUMN_ALIASES = {
    "브랜드": "brand_name",
    "브랜드명": "brand_name",
    "brand": "brand_name",
    "상품군": "category_name",
    "카테고리": "category_name",
    "category": "category_name",
    "상품명": "product_name",
    "product": "product_name",
    "판매채널": "channel",
    "채널": "channel",
    "channel_name": "channel",
    "별점": "rating",
    "평점": "rating",
    "score": "rating",
    "리뷰": "review_text",
    "리뷰내용": "review_text",
    "리뷰 내용": "review_text",
    "후기": "review_text",
    "내용": "review_text",
    "작성일": "review_date",
    "리뷰일": "review_date",
    "date": "review_date",
    "옵션": "option_name",
    "옵션명": "option_name",
    "작성자": "writer",
    "구매자": "writer",
    "리뷰URL": "review_url",
    "리뷰 URL": "review_url",
    "주문번호": "order_no",
    "이미지여부": "image_yn",
    "답변여부": "reply_yn",
}


@dataclass
class KeywordDict:
    """키워드 사전 컨테이너. 운영 시 DB 테이블로 치환 가능."""

    negative: list[str] = field(default_factory=list)
    positive: list[str] = field(default_factory=list)
    strong_negative: list[str] = field(default_factory=list)
    issue_rules: dict[str, list[str]] = field(default_factory=dict)


DEFAULT_KEYWORDS = KeywordDict(
    negative=[
        "냄새", "화학냄새", "새제품냄새", "불량", "파손", "늦", "지연", "환불", "교환", "실망",
        "마감", "들뜸", "틈새", "변색", "오염", "찢", "찍힘", "약함", "흔들", "누락",
        "불편", "비싸", "후회", "별로", "안좋", "안 좋", "문제", "하자", "스크래치",
    ],
    positive=[
        "좋", "만족", "추천", "예쁘", "두껍", "푹신", "폭신", "안심", "깔끔", "고급",
        "편해", "튼튼", "빠르", "귀엽", "부드", "안전", "재구매", "감사",
    ],
    strong_negative=["환불", "교환", "불량", "파손", "하자", "실망", "후회"],
    issue_rules={
        "제품 품질": ["불량", "파손", "하자", "찢", "찍힘", "변색", "오염", "내구", "약함", "스크래치"],
        "냄새/소재": ["냄새", "화학냄새", "새제품냄새", "소재", "환기"],
        "물류/배송": ["배송", "늦", "지연", "오배송", "누락", "포장", "박스"],
        "설치/조립": ["조립", "설치", "설명서", "나사", "부품", "고정"],
        "시공/방문": ["시공", "기사", "방문", "들뜸", "틈새"],
        "상세페이지": ["색상", "사이즈", "크기", "다름", "사진", "옵션", "구성"],
        "가격/프로모션": ["비싸", "가격", "할인", "쿠폰", "가성비"],
        "CS/상담": ["상담", "응대", "고객센터", "문의", "답변"],
    },
)


# ──────────────────────────────────────────────────────────────
# 기초 유틸
# ──────────────────────────────────────────────────────────────

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for col in df.columns:
        clean = str(col).strip()
        renamed[col] = COLUMN_ALIASES.get(clean, clean)
    return df.rename(columns=renamed)


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def find_hits(text: str, keywords: Iterable[str]) -> list[str]:
    text = normalize_text(text)
    if not text:
        return []
    return [kw for kw in keywords if kw in text]


def mask_writer(value) -> str:
    value = normalize_text(value)
    if not value:
        return ""
    if len(value) <= 2:
        return value[0] + "*"
    return value[0] + "*" * (len(value) - 2) + value[-1]


def _parse_rating(value) -> float | None:
    try:
        r = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(r):
        return None
    return r


# ──────────────────────────────────────────────────────────────
# 분류 로직
# ──────────────────────────────────────────────────────────────

def classify_issue(text: str, rules: dict[str, list[str]]) -> tuple[str, list[str]]:
    """
    이슈 유형 분류.

    반환: (primary_issue, all_matched_issues)

    hit 수가 가장 많은 카테고리를 primary로 선정.
    hit 수가 같으면 rules 딕셔너리 선언 순서(중요도)를 따름.
    """
    text = normalize_text(text)
    if not text:
        return "기타", []

    scored: list[tuple[str, int]] = []
    for issue, kws in rules.items():
        hits = sum(1 for kw in kws if kw in text)
        if hits > 0:
            scored.append((issue, hits))

    if not scored:
        return "기타", []

    # Python sort는 stable → 선언순서가 tie-break
    scored.sort(key=lambda x: -x[1])
    all_issues = [s[0] for s in scored]
    return scored[0][0], all_issues


def classify_sentiment(rating, text: str, kw: KeywordDict) -> str:
    """
    감성 분류. rating이 NaN이면 키워드로만 판단.

    반환: "긍정" | "부정" | "중립"
    """
    text = normalize_text(text)
    neg_hits = find_hits(text, kw.negative)
    pos_hits = find_hits(text, kw.positive)
    r = _parse_rating(rating)

    if r is not None:
        if r <= 2:
            return "부정"
        if r >= 4 and len(neg_hits) >= 2:
            return "부정"
        if r >= 4 and not neg_hits:
            return "긍정"
        if r == 3 and neg_hits:
            return "부정"
        if r == 3 and pos_hits and not neg_hits:
            return "긍정"
        if r >= 4:
            return "긍정"
        return "중립"

    # 평점 없음 → 키워드
    if len(neg_hits) >= 2:
        return "부정"
    if neg_hits and not pos_hits:
        return "부정"
    if pos_hits and not neg_hits:
        return "긍정"
    return "중립"


def classify_risk(rating, text: str, kw: KeywordDict) -> str:
    """
    위험도: 높음 | 중간 | 낮음.

    rating NaN이면 키워드만으로 판단.
    """
    text = normalize_text(text)
    neg_hits = find_hits(text, kw.negative)
    strong_hits = [t for t in kw.strong_negative if t in text]
    r = _parse_rating(rating)

    if r is not None:
        if r <= 2 and strong_hits:
            return "높음"
        if r <= 2:
            return "높음"
        if r == 3 and neg_hits:
            return "중간"
        if r >= 4 and len(neg_hits) >= 2:
            return "중간"
        return "낮음"

    # 평점 없음
    if strong_hits:
        return "높음"
    if neg_hits:
        return "중간"
    return "낮음"


# ──────────────────────────────────────────────────────────────
# 중복 판정 키 (RFP 8.1)
# ──────────────────────────────────────────────────────────────

def dedup_key(row) -> str:
    """
    RFP 8.1 중복 판정 기준을 fallback 체인으로 구현.

    우선순위 (안정성 높은 순):
    1. review_url이 있으면 → url|{url}
    2. order_no가 있으면 → ord|{channel}|{order_no}|{text_hash}
    3. 기본 → gen|{channel}|{product}|{date}|{text_hash}
    """
    channel = normalize_text(row.get("channel"))
    product = normalize_text(row.get("product_name"))
    review_text = normalize_text(row.get("review_text"))
    review_date = row.get("review_date")
    if hasattr(review_date, "strftime"):
        date_str = review_date.strftime("%Y-%m-%d")
    else:
        date_str = normalize_text(review_date)

    order_no = normalize_text(row.get("order_no"))
    review_url = normalize_text(row.get("review_url"))

    text_hash = hashlib.sha1(review_text.encode("utf-8")).hexdigest()[:16]

    if review_url:
        return f"url|{review_url}"
    if order_no:
        return f"ord|{channel}|{order_no}|{text_hash}"
    return f"gen|{channel}|{product}|{date_str}|{text_hash}"


# ──────────────────────────────────────────────────────────────
# 전체 enrich 파이프라인
# ──────────────────────────────────────────────────────────────

ENRICHED_EXTRA_COLUMNS = [
    "writer_masked",
    "negative_keywords",
    "positive_keywords",
    "sentiment",
    "risk_level",
    "issue_type",
    "issue_types_all",
    "status",
    "assignee",
    "memo",
    "created_at",
    "dedup_key",
    "review_date_was_missing",
]


def enrich_reviews(df: pd.DataFrame, kw: KeywordDict | None = None) -> pd.DataFrame:
    if kw is None:
        kw = DEFAULT_KEYWORDS

    df = normalize_columns(df).copy()

    for col in REQUIRED_COLUMNS + OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[REQUIRED_COLUMNS + OPTIONAL_COLUMNS]

    # rating: 숫자로 변환하되 NaN 유지
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    valid = df["rating"].notna()
    df.loc[valid, "rating"] = df.loc[valid, "rating"].clip(0, 5)

    df["review_text"] = df["review_text"].fillna("").astype(str).str.strip()
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    df["review_date_was_missing"] = df["review_date"].isna()
    df["review_date"] = df["review_date"].fillna(pd.Timestamp.today().normalize())

    for col in ["brand_name", "category_name", "product_name", "channel", "option_name", "writer"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["writer_masked"] = df["writer"].apply(mask_writer)

    df["negative_keywords"] = df["review_text"].apply(
        lambda t: ", ".join(find_hits(t, kw.negative))
    )
    df["positive_keywords"] = df["review_text"].apply(
        lambda t: ", ".join(find_hits(t, kw.positive))
    )

    df["sentiment"] = df.apply(
        lambda r: classify_sentiment(r["rating"], r["review_text"], kw), axis=1
    )
    df["risk_level"] = df.apply(
        lambda r: classify_risk(r["rating"], r["review_text"], kw), axis=1
    )

    issue_results = df["review_text"].apply(lambda t: classify_issue(t, kw.issue_rules))
    df["issue_type"] = issue_results.apply(lambda x: x[0])
    df["issue_types_all"] = issue_results.apply(lambda x: ", ".join(x[1]))

    df["status"] = "미확인"
    df["assignee"] = ""
    df["memo"] = ""
    df["created_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    df["dedup_key"] = df.apply(dedup_key, axis=1)

    return df


def keyword_count(df: pd.DataFrame, keyword_col: str) -> pd.DataFrame:
    """콤마 구분 키워드 컬럼을 풀어서 빈도표 반환."""
    values: list[str] = []
    series = df.get(keyword_col)
    if series is None:
        return pd.DataFrame(columns=["keyword", "count"])
    for item in series.dropna().astype(str):
        values.extend([x.strip() for x in item.split(",") if x.strip()])
    if not values:
        return pd.DataFrame(columns=["keyword", "count"])
    counts = pd.Series(values).value_counts().reset_index()
    counts.columns = ["keyword", "count"]
    return counts


def deduplicate(existing_keys: set[str], new_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    existing_keys(기존 DB의 dedup_key 집합) 기준으로 신규·중복 분리.

    반환: (inserted_df, duplicated_df)
    """
    new_df = new_df.copy()
    new_df["_is_dup"] = new_df["dedup_key"].astype(str).isin(existing_keys)
    inserted = new_df[~new_df["_is_dup"]].drop(columns=["_is_dup"])
    duplicated = new_df[new_df["_is_dup"]].drop(columns=["_is_dup"])
    return inserted, duplicated
