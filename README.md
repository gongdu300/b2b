# 🏭 SmartInventory  
**B2B 고객 제품 수요량 예측 기반 재고 최적화 플랫폼**

---

## 📌 프로젝트 개요
SmartInventory는 B2B 고객의 제품 수요 데이터를 기반으로  
**AI 수요예측 모델을 활용해 재고를 최적화**하는 웹 기반 플랫폼입니다.  

- 데이터 업로드부터 DB 저장, 전처리, 예측까지 **원스톱 처리**
- **제품코드·기준일(T일)** 기준으로 **예측기간별 수요량을 시각화**
- **Chart.js** 기반 시각화 및 **FastAPI** 백엔드 연동
- **Gemini API** 를 활용한 컬럼/테이블명 자동 번역 및 표준화 기능 포함

---

## 🧩 주요 기능
| 구분 | 기능 설명 |
|------|------------|
| 📤 데이터 업로드 | CSV 업로드 → DB 저장 (테이블명 자동 변환) |
| 🧪 데이터 전처리 | 결측치 처리, 스케일링, 불필요 컬럼 제거 |
| 📈 수요 예측 | 타깃 컬럼, 기준일 컬럼, 제품코드 컬럼, 예측기간 선택 후 예측 수행 |
| 📊 시각화 | Chart.js 기반 제품코드별 예측 수요량 그래프 |
| 🌐 번역 자동화 | Google Gemini API 이용, 한글 컬럼명 → 영문명 변환 |
| 💾 데이터 다운로드 | 처리된 테이블 CSV 다운로드 지원 |

---

## ⚙️ 기술 스택
| 구분 | 사용 기술 |
|------|------------|
| Backend | FastAPI, Python, Pandas, SQLAlchemy |
| Frontend | HTML, JS (Chart.js, Fetch API), CSS |
| Database | MySQL (DB_NAME=data_platform) |
| AI Model | Google Gemini API (컬럼/테이블명 번역) |
| Infra | GitHub, 로컬 실행 / AWS or Vercel 배포 예정 |

---

## 🧠 구조
project_root/
├── gemini # gemini 번역 모듈
├───── init.py
├───── config.py # 환경설정 (DB, API key 등)
├───── translator.py # Gemini 번역 모듈
├───── utils.py # 캐시 및 문자열 처리 함수
├── test_csv_bundle # 업로드 테스트 데이터셋 폴더
├── main.py # FastAPI 백엔드 진입점
├── index.html # 프론트엔드 UI
├── script.js # 프론트 로직 (Fetch + Chart.js)
├── style.css # 프론트 디자인
├── .env # 환경변수 (DB, Gemini key)
├── requirements.txt # 패키지 설치
└── README.md

## 🚀 실행 방법
### 1️⃣ 가상환경 및 패키지 설치
```bash
conda create -n smartinventory python=3.10
conda activate smartinventory
pip install -r requirements.txt
uvicorn main:app