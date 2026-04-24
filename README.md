# 꿈비그룹 통합 리뷰 인사이트 (Kumbi Review Insight)

네이버 스마트스토어·쿠팡·오늘의집·자사몰의 리뷰를 엑셀/CSV로 업로드해 브랜드·상품군·상품·채널 단위로 통합 분석하는 내부 관리자 시스템.

본 저장소는 [RFP v1.0](https://example.invalid) 기반 1차 MVP 구현이며, Streamlit + SQLite로 경량 운영 가능한 수준까지 다듬어져 있습니다.

## 주요 기능

- **로그인·권한 관리**: admin / manager / viewer 3단계 역할. bcrypt 해시 비밀번호.
- **엑셀·CSV 업로드**: 한글·영문 컬럼명 자동 매핑, 필수 컬럼 검증, 미리보기.
- **자동 분류**: 감성(긍정/부정/중립), 위험도(높음/중간/낮음), 이슈 유형 8종 (제품 품질, 냄새/소재, 물류/배송, 설치/조립, 시공/방문, 상세페이지, 가격/프로모션, CS/상담).
- **중복 제거**: `review_url` → `order_no+text_hash` → `channel+product+date+text_hash` fallback 체인으로 안정적으로 판별 (RFP 8.1).
- **업로드 이력**: 성공·중복·실패 건수 + 사용자·파일명 로깅.
- **대시보드**: 전체/브랜드/채널/이슈 유형별 지표, 부정 키워드 TOP 10.
- **키워드 사전 DB 관리**: 관리자가 UI에서 키워드·이슈 룰 추가/삭제 가능 (재배포 불필요).
- **리뷰 편집**: 처리 상태, 담당자, 위험도, 메모 인라인 편집.
- **엑셀 다운로드**: 필터 결과 그대로 xlsx로 추출.

## 기술 스택

- **Language**: Python 3.12
- **UI**: Streamlit 1.39
- **DB**: SQLite (WAL 모드) + Railway Volume
- **분석**: pandas 2.2
- **차트**: Plotly
- **인증**: bcrypt

## 프로젝트 구조

```
.
├── app.py              # Streamlit UI (메뉴·화면)
├── classifier.py       # 분류 로직 (감성·위험도·이슈·중복키). Streamlit 독립.
├── db.py               # SQLite 계층 (스키마·CRUD·트랜잭션)
├── auth.py             # 로그인·사용자·bcrypt
├── tests/
│   └── test_classifier.py   # 25개 단위 테스트
├── sample_reviews.csv  # 20행 샘플
├── requirements.txt
├── railway.toml        # Railway 배포 설정
├── nixpacks.toml       # Python 3.12 고정
├── .env.example        # 환경변수 템플릿
└── .gitignore
```

## 로컬 실행

```bash
python -m venv .venv
source .venv/bin/activate                # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 최초 관리자 계정 설정 (선택. 미지정 시 admin@example.com / changeme!)
export ADMIN_EMAIL=admin@octoworks.kr
export ADMIN_PASSWORD=yourpassword

streamlit run app.py
```

브라우저에서 자동으로 열리는 `http://localhost:8501` 접속 후 로그인.

## Railway 배포

1. GitHub에 push.
2. Railway 대시보드 → **New Project** → **Deploy from GitHub repo** → 본 저장소 선택.
3. **Volume 추가**: Mount Path = `/data`.
4. **Environment Variables**:
   ```
   DB_PATH=/data/reviews.db
   ADMIN_EMAIL=admin@octoworks.kr
   ADMIN_PASSWORD=<강력한 비밀번호>
   ```
5. Deploy. `railway.toml`이 자동으로 `streamlit run`을 기동.

## 테스트

```bash
python -m unittest tests.test_classifier -v
```

총 25개 테스트. 분류·중복 판정·키워드 카운트·엔드투엔드 enrich를 검증합니다.

## 업로드 CSV 스펙

### 필수 컬럼

| 컬럼명 | 설명 | 한글 별칭 |
|---|---|---|
| `brand_name` | 브랜드명 | 브랜드, 브랜드명 |
| `category_name` | 상품군 | 상품군, 카테고리 |
| `product_name` | 상품명 | 상품명 |
| `channel` | 판매채널 | 채널, 판매채널 |
| `rating` | 평점 (0~5) | 별점, 평점 |
| `review_text` | 리뷰 내용 | 리뷰, 리뷰내용, 후기 |
| `review_date` | 작성일 (YYYY-MM-DD) | 작성일, 리뷰일 |

### 선택 컬럼

| 컬럼명 | 설명 |
|---|---|
| `option_name` | 옵션명 |
| `writer` | 작성자 (저장 시 자동 마스킹) |
| `review_url` | 원본 URL (있으면 최우선 dedup 키) |
| `order_no` | 주문번호 |
| `image_yn` | 이미지 리뷰 여부 |
| `reply_yn` | 판매자 답변 여부 |

## RFP 대비 커버리지 (1차 MVP)

| RFP 요구 | 상태 |
|---|---|
| 6.1 조직·브랜드·상품 관리 | ▲ 부분 (리뷰 내 포함, 별도 마스터 미구현) |
| 6.2 리뷰 업로드 + 컬럼 매핑 + 중복 제거 | ✅ |
| 6.3 리뷰 통합 조회·필터·상태 변경 | ✅ |
| 6.4 부정 리뷰 자동 분류 + 위험도 | ✅ |
| 6.5 대시보드 | ✅ |
| 6.6 키워드 사전 관리 | ✅ (DB·UI) |
| 6.7 개선 과제 관리 | ⬜ 2차 |
| 8.1 중복 리뷰 3단 fallback | ✅ |
| 10 보안·개인정보 | ▲ bcrypt·마스킹 완료, HTTPS는 Railway 기본 |
| 12 동시 20~50명, 1회 1만 행 업로드 | ✅ (트랜잭션 기반 bulk insert) |

## 2차 확장 계획

- AI 리뷰 요약 (Anthropic API 연동)
- 월간 PDF 리포트
- 카카오 알림톡·이메일 알림
- 채널별 반자동 수집 (cafe24-sales-manager 자산 재활용)
- 상세페이지 개선 과제 관리
- 조직→브랜드→상품군→상품 마스터 별도 관리 화면
- PostgreSQL 전환 (동시성·백업 수준 상승 시)

## 라이선스

Proprietary — 주식회사 옥토웍스 / 꿈비그룹 내부 사용.
