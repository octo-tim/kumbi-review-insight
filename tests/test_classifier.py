"""
classifier.py 단위 테스트.

실행:
    python -m unittest tests.test_classifier -v
"""
import unittest
from pathlib import Path

import pandas as pd

from classifier import (
    DEFAULT_KEYWORDS,
    classify_issue,
    classify_risk,
    classify_sentiment,
    dedup_key,
    deduplicate,
    enrich_reviews,
    find_hits,
    keyword_count,
    mask_writer,
    normalize_columns,
)


class TestNormalize(unittest.TestCase):
    def test_column_alias_korean(self):
        df = pd.DataFrame({"브랜드": ["꿈비"], "별점": [5], "리뷰내용": ["좋음"]})
        out = normalize_columns(df)
        self.assertIn("brand_name", out.columns)
        self.assertIn("rating", out.columns)
        self.assertIn("review_text", out.columns)

    def test_mask_writer(self):
        self.assertEqual(mask_writer("김철수"), "김*수")
        self.assertEqual(mask_writer("홍길"), "홍*")
        self.assertEqual(mask_writer(""), "")
        self.assertEqual(mask_writer("가나다라마"), "가***마")


class TestKeywordHits(unittest.TestCase):
    def test_basic_hit(self):
        hits = find_hits("냄새가 너무 심하다", ["냄새", "파손"])
        self.assertEqual(hits, ["냄새"])

    def test_empty_text(self):
        self.assertEqual(find_hits("", ["냄새"]), [])
        self.assertEqual(find_hits(None, ["냄새"]), [])


class TestClassifyIssue(unittest.TestCase):
    def test_multi_category_picks_majority(self):
        # '파손'(품질1) + '포장'(물류1) + '박스'(물류1) → 물류 2hits → primary=물류
        primary, all_ = classify_issue(
            "박스 파손되고 포장이 엉망이었어요", DEFAULT_KEYWORDS.issue_rules
        )
        self.assertEqual(primary, "물류/배송")
        self.assertIn("물류/배송", all_)
        self.assertIn("제품 품질", all_)

    def test_no_match(self):
        primary, all_ = classify_issue("보통이에요", DEFAULT_KEYWORDS.issue_rules)
        self.assertEqual(primary, "기타")
        self.assertEqual(all_, [])

    def test_empty_text(self):
        primary, _ = classify_issue("", DEFAULT_KEYWORDS.issue_rules)
        self.assertEqual(primary, "기타")

    def test_multi_hit_picks_highest(self):
        primary, _ = classify_issue(
            "조립이 어렵고 설명서가 부족하고 설치도 복잡",
            DEFAULT_KEYWORDS.issue_rules,
        )
        self.assertEqual(primary, "설치/조립")


class TestSentiment(unittest.TestCase):
    def test_rating_low(self):
        self.assertEqual(classify_sentiment(1, "괜찮음", DEFAULT_KEYWORDS), "부정")
        self.assertEqual(classify_sentiment(2, "", DEFAULT_KEYWORDS), "부정")

    def test_rating_high_positive(self):
        self.assertEqual(
            classify_sentiment(5, "만족해요 좋아요", DEFAULT_KEYWORDS), "긍정"
        )

    def test_rating_high_with_many_negatives(self):
        result = classify_sentiment(
            5, "불량이고 파손되어 왔습니다 교환 요청합니다", DEFAULT_KEYWORDS
        )
        self.assertEqual(result, "부정")

    def test_rating_nan_kept_as_neutral_or_keyword(self):
        # 원본 버그: NaN이 fillna(0)으로 떨어져 모두 부정으로 분류됐음
        self.assertEqual(
            classify_sentiment(float("nan"), "정말 좋아요 만족", DEFAULT_KEYWORDS),
            "긍정",
        )
        self.assertEqual(
            classify_sentiment(None, "", DEFAULT_KEYWORDS), "중립"
        )
        self.assertEqual(classify_sentiment("", "", DEFAULT_KEYWORDS), "중립")

    def test_rating_3_with_negative(self):
        self.assertEqual(
            classify_sentiment(3, "냄새가 너무 납니다", DEFAULT_KEYWORDS), "부정"
        )


class TestRisk(unittest.TestCase):
    def test_low_rating_strong_negative(self):
        self.assertEqual(
            classify_risk(1, "환불 요청", DEFAULT_KEYWORDS), "높음"
        )

    def test_high_rating(self):
        self.assertEqual(
            classify_risk(5, "좋아요", DEFAULT_KEYWORDS), "낮음"
        )

    def test_rating_nan(self):
        self.assertEqual(
            classify_risk(float("nan"), "환불해주세요", DEFAULT_KEYWORDS), "높음"
        )
        # 원본 버그: NaN + 일반 텍스트가 '높음'으로 잘못 분류됐었음
        self.assertEqual(
            classify_risk(float("nan"), "보통이에요", DEFAULT_KEYWORDS), "낮음"
        )


class TestDedupKey(unittest.TestCase):
    def test_url_priority(self):
        row = pd.Series({
            "channel": "네이버",
            "product_name": "매트A",
            "review_text": "좋아요",
            "review_date": pd.Timestamp("2026-04-01"),
            "order_no": "N001",
            "review_url": "https://example.com/r/123",
        })
        self.assertTrue(dedup_key(row).startswith("url|"))

    def test_order_no_fallback(self):
        row = pd.Series({
            "channel": "쿠팡",
            "product_name": "매트A",
            "review_text": "좋아요",
            "review_date": pd.Timestamp("2026-04-01"),
            "order_no": "C001",
            "review_url": "",
        })
        self.assertTrue(dedup_key(row).startswith("ord|쿠팡|C001|"))

    def test_generic_fallback(self):
        row = pd.Series({
            "channel": "스마트스토어",
            "product_name": "매트A",
            "review_text": "좋아요",
            "review_date": pd.Timestamp("2026-04-01"),
            "order_no": "",
            "review_url": "",
        })
        self.assertTrue(dedup_key(row).startswith("gen|스마트스토어|매트A|2026-04-01|"))

    def test_same_starting_text_not_collapsed(self):
        """원본 버그: 앞 100자 slicing으로 거짓 중복 발생."""
        base = {
            "channel": "쿠팡",
            "product_name": "매트A",
            "review_date": pd.Timestamp("2026-04-01"),
            "order_no": "",
            "review_url": "",
        }
        r1 = pd.Series({**base, "review_text": "배송 빠르고 좋아요. 아이가 뛰어도 푹신해서 안심입니다."})
        r2 = pd.Series({**base, "review_text": "배송 빠르고 좋아요. 그런데 냄새가 심해서 환기가 필요했어요."})
        self.assertNotEqual(dedup_key(r1), dedup_key(r2))


class TestKeywordCount(unittest.TestCase):
    def test_basic_count(self):
        df = pd.DataFrame({"negative_keywords": ["냄새, 파손", "냄새", "지연, 환불"]})
        out = keyword_count(df, "negative_keywords")
        self.assertListEqual(list(out.columns), ["keyword", "count"])
        mapping = dict(zip(out["keyword"], out["count"]))
        self.assertEqual(mapping["냄새"], 2)
        self.assertEqual(mapping["파손"], 1)

    def test_empty_col(self):
        df = pd.DataFrame({"negative_keywords": []})
        out = keyword_count(df, "negative_keywords")
        self.assertListEqual(list(out.columns), ["keyword", "count"])
        self.assertEqual(len(out), 0)

    def test_missing_col(self):
        df = pd.DataFrame({"other": [1, 2, 3]})
        out = keyword_count(df, "negative_keywords")
        self.assertListEqual(list(out.columns), ["keyword", "count"])


class TestEnrichReviewsEndToEnd(unittest.TestCase):
    def test_sample_csv(self):
        csv_path = Path(__file__).parent.parent / "sample_reviews.csv"
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        enriched = enrich_reviews(df)
        for c in ["sentiment", "risk_level", "issue_type", "dedup_key"]:
            self.assertIn(c, enriched.columns)
        self.assertEqual(len(enriched), len(df))
        self.assertTrue(set(enriched["sentiment"]).issubset({"긍정", "부정", "중립"}))
        self.assertTrue(set(enriched["risk_level"]).issubset({"높음", "중간", "낮음"}))


class TestDeduplicate(unittest.TestCase):
    def test_split(self):
        new_df = pd.DataFrame({"dedup_key": ["a", "b", "c"], "review_text": ["x", "y", "z"]})
        inserted, dup = deduplicate({"a", "c"}, new_df)
        self.assertEqual(len(inserted), 1)
        self.assertEqual(inserted.iloc[0]["dedup_key"], "b")
        self.assertEqual(len(dup), 2)


if __name__ == "__main__":
    unittest.main()
