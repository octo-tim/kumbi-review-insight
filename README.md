# 꿈비그룹 통합 리뷰 인사이트 (Kumbi Review Insight)

네이버 스마트스토어·쿠팡·오늘의집·자사몰의 리뷰를 엑셀/CSV로 업로드해 브랜드·상품군·상품·채널 단위로 통합 분석하는 내부 관리자 시스템.

RFP v1.0 기반 1차 MVP. Streamlit + PostgreSQL로 경량 운영 가능한 수준까지 다듬어져 있습니다.

## 주요 기능

- **로그인·권한 관리**: admin / manager / viewer 3단계 역할. bcrypt 해시 비밀번호.
- **엑셀·CSV 업로드**: 한글·영문 컬럼명 자동 매핑, 필수 컬럼 검증, 미리보기.
- **자동 분류**: 감성(긍정/부정/중립), 위험도(높음/중간/낮음), 이슈 유형 8종 (제품 품질, 냄새/소재, 물류/배송, 설치/조립, 시공/방문, 상세페이지, 가격/프로모션, CS/상담).
- **중복 제거**: `review_url` → `order_no+text_hash` → `channel+product+date+text_hash` fallback 체인 (RFP 8.1).
- **업로드 이력**: 성공·중복·실패 건수 + 사용자·파일명 로깅.
- **대시보드**: 전체/브랜드/채널/이슈 유형별 지표, 부정 키워드 TOP 10.
- **키워드 사전 DB 관리**: 관리자가 UI에서 키워드·이슈 룰 추가/삭제 가능 (재배포 불필요).
- **리뷰 편집**: 처리 상태·담당자·위험도·메모 인라인 편집.
- **엑셀 다운로드**: 필터 결과 그대로 xlsx로 추출.

## 기술 스택

- **Language**: Python 3.12
- **UI**: Streamlit 1.39
- **DB**: PostgreSQL (psycopg 3 + SQLAlchemy 2.0)
- **분석**: pandas 2.2, Plotly
- **인증**: bcrypt

## 프로젝트 구조

```
.
├── app.py              # Streamlit UI
├── classifier.py       # 분류 로직 (감성·위험도·이슈·중복키). DB 독립.
├── db.py               # Postgres 계층 (스키마·CRUD·커넥션 풀)
├── auth.py             # 로그인·사용자·bcrypt
├── tests/
│   └── test_classifier.py   # 25개 단위 테스트
├── sample_reviews.csv  # 20행 샘플
├── requirements.txt
├── Procfile            # Railway 시작 명령
├── railway.toml
├── nixpacks.toml       # Python 3.12 고정
├── .env.example
└── .gitignore
```

## 로컬 실행

### 1. Postgres 준비

```bash
# macOS (Homebrew 기준)
brew install postgresql@16
brew services start postgresql@16
createdb kumbi
```

### 2. 앱 실행

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export DATABASE_URL="postgresql://$(whoami)@localhost:5432/kumbi"
export ADMIN_EMAIL="admin@octoworks.kr"
export ADMIN_PASSWORD="yourpassword"

streamlit run app.py
```

브라우저에서 `http://localhost:8501` 접속 후 로그인.

## Railway 배포

### 1. GitHub 저장소 연결

Railway 대시보드 → **New Project** → **Deploy from GitHub repo** → `octo-tim/kumbi-review-insight`.

### 2. Postgres 플러그인 추가

프로젝트 캔버스에서 **+ Create** → **Database** → **PostgreSQL**.
프로젝트 내에 Postgres 서비스와 앱 서비스가 나란히 생성됩니다.

### 3. 환경변수 연결

앱 서비스 → **Variables** 탭에서 아래 추가:

```
DATABASE_URL=${{Postgres.DATABASE_URL}}
ADMIN_EMAIL=admin@octoworks.kr
ADMIN_PASSWORD=<강력한 비밀번호>
```

`${{Postgres.DATABASE_URL}}`는 Railway의 참조 문법으로, Postgres 서비스의 연결 문자열이 자동 주입됩니다.

### 4. 포트·도메인

Procfile이 포트 `8080`에 고정 바인딩합니다. Networking 탭에서 Target Port는 **8080**으로 설정하시면 됩니다.

### 5. 첫 배포

Push 하면 자동으로 빌드·배포가 진행됩니다. Logs에서:

```
You can now view your Streamlit app in your browser.
```

가 보이면 성공.

## 테스트

```bash
python -m unittest tests.test_classifier -v
```

총 25개 테스트 (분류·중복 판정·키워드 카운트·엔드투엔드 enrich).

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
| 12 동시 20~50명, 1회 1만 행 업로드 | ✅ (커넥션 풀 + 트랜잭션 기반 bulk insert) |

## 2차 확장 계획

- AI 리뷰 요약 (Anthropic API 연동)
- 월간 PDF 리포트
- 카카오 알림톡·이메일 알림
- 채널별 반자동 수집 (cafe24-sales-manager 자산 재활용)
- 상세페이지 개선 과제 관리
- 조직→브랜드→상품군→상품 마스터 별도 관리 화면

## 라이선스

Proprietary — 주식회사 옥토웍스 / 꿈비그룹 내부 사용.
