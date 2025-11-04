from __future__ import annotations
import os
import io
import re
import csv
import uuid
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
import pathlib, tempfile
import pandas as pd

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from pydantic import BaseModel
from typing import List, Any
# --- DB Engine (MySQL) ---
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from gemini import translate_table_name as _translate_table_name

# --- [ADD] 로그 레벨 DEBUG로 올리기 (ENV로도 제어 가능) ---
import logging

import re
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError, IntegrityError, DataError

import pandas as pd
import numpy as np
from pydantic import BaseModel
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.metrics import mean_absolute_error
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor


_ASCII_ID_RE = re.compile(r"^[A-Za-z0-9_]+$")

def _is_ascii_identifier(s: str) -> bool:
    """영문/숫자/언더스코어만으로 구성되면 True (빈 문자열은 False)."""
    if not s:
        return False
    return bool(_ASCII_ID_RE.fullmatch(str(s)))


# --- LLM (Gemini) optional ---
try:
    from gemini.config import GEMINI_API_KEY, session, pick_model, api_url
    _HAS_LLM = bool(GEMINI_API_KEY)
except Exception:
    _HAS_LLM = False

def llm_advise_schema(headers, dtypes, samples, targets):
    """
    스키마/샘플/타깃을 요약해서 LLM에 '시계열 vs 일반 회귀' 추천만 요청.
    실패하면 None 반환.
    """
    if not _HAS_LLM:
        return None
    payload_txt = (
        "You are a data science assistant. Decide for each target whether "
        "the dataset should be treated as TIME_SERIES or TABULAR REGRESSION. "
        "Return strict JSON with keys per target and value in {\"time_series\"|\"regression\"}.\n\n"
        f"Headers: {headers}\n"
        f"Dtypes: {dtypes}\n"
        f"Targets: {targets}\n"
        f"Sample rows (first 5): {samples}"
    )
    model = pick_model()
    try:
        r = session.post(
            api_url(model),
            params={"key": GEMINI_API_KEY},
            json={"contents":[{"parts":[{"text":payload_txt}]}]},
            timeout=12
        )
        if r.status_code != 200:
            return None
        js = r.json()
        txt = js.get("candidates",[{}])[0].get("content",{}).get("parts",[{}])[0].get("text","{}")
        # 매우 관대한 JSON 파싱
        import json, re
        txt_clean = re.sub(r"```json|```", "", txt).strip()
        return json.loads(txt_clean)
    except Exception:
        return None
    
# -------------------------------------------------
# 일반 회귀용 빌더 (시간컬럼 없을 때 fallback)
# -------------------------------------------------
def _build_regression(df, target_col: str):
    """
    df : pandas DataFrame (컬럼에 target_col이 포함돼 있어야 함)
    target_col : 예측하려는 숫자 컬럼

    리턴:
      model  : 학습된 모델 객체
      meta   : 학습 정보(dict)
    """
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestRegressor
    import numpy as np

    # 1) 타깃/피처 분리
    if target_col not in df.columns:
        raise ValueError(f"target column '{target_col}' not found in dataframe")

    y = df[target_col]
    X = df.drop(columns=[target_col])

    # 2) 숫자형만 우선 사용 (날짜, 문자열 들어오면 깨지니까)
    num_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]
    if not num_cols:
        # 숫자 피처가 하나도 없으면 예측 자체가 의미 없으니 실패 리턴
        return None, {
            "ok": False,
            "reason": "no numeric feature columns to train on",
            "target": target_col,
        }

    X = X[num_cols].copy()
    # 타깃도 숫자로 바꿈
    y = pd.to_numeric(y, errors="coerce")
    mask = y.notna()
    X = X.loc[mask]
    y = y.loc[mask]

    if len(X) < 10:
        return None, {
            "ok": False,
            "reason": "not enough rows for regression",
            "rows": int(len(X)),
            "target": target_col,
        }

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    score = model.score(X_test, y_test)

    meta = {
        "ok": True,
        "target": target_col,
        "n_samples": int(len(df)),
        "n_features": len(num_cols),
        "features": num_cols,
        "r2": float(score),
    }
    return model, meta


# main.py 상단 유틸들 근처
def _resolve_target_name(df: pd.DataFrame, y: str) -> str | None:
    """
    사용자가 선택한 y가 df.columns에 정확히 없으면
    - y와 동일하거나
    - y + '__숫자' (중복 유니크화된 케이스)
    중 첫 번째를 자동 선택.
    """
    if y in df.columns:
        return y
    cand = [c for c in df.columns if c == y or c.startswith(f"{y}__")]
    return cand[0] if cand else None

# === [ADD] 컬럼 안전 픽커 ===
def _pick_existing_col(df: pd.DataFrame, base: str) -> str | None:
    """
    df.columns 안에 base가 없으면
    - 대소문자 무시 동일
    - base + '__숫자' (중복 유니크화) 
    - 공백/양끝 공백 차이
    중에서 첫 매치를 반환. 없으면 None.
    """
    cols = list(df.columns)
    # 1) 정확히
    if base in df.columns:
        return base

    # 2) strip / casefold 동일
    base_norm = str(base).strip().casefold()
    for c in cols:
        if str(c).strip().casefold() == base_norm:
            return c

    # 3) base__k (중복 유니크화)
    for c in cols:
        if str(c).startswith(f"{base}__"):
            return c

    # 4) 흔한 변형(공백 → 언더스코어)
    base_us = str(base).replace(" ", "_")
    for c in cols:
        if str(c) == base_us:
            return c

    return None





LOG_LEVEL = os.getenv("LOG_LEVEL", "debug").upper()  # 필요시 환경변수로 조절
_level = getattr(logging, LOG_LEVEL, logging.DEBUG)

# 루트 로거 및 주요 로거들 레벨 설정
logging.basicConfig(level=_level)
for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
    logging.getLogger(name).setLevel(_level)

# (선택) SQLAlchemy 등도 보고 싶으면:
# logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

# [ADD] SQL 에러 휴머니즈
from sqlalchemy.exc import ProgrammingError  # 파일 상단 임포트에 추가

def _ensure_unique_names(names: list[str]) -> list[str]:
    seen = {}
    out = []
    for n in names:
        base = n
        if base not in seen:
            seen[base] = 1
            out.append(base)
            continue
        i = seen[base] + 1
        cand = f"{base}__{i}"
        while cand in seen:
            i += 1
            cand = f"{base}__{i}"
        seen[base] = i
        seen[cand] = 1
        out.append(cand)
    return out

# [ADD] ---- 샘플 기반 타입 추론 & 행크기 조절 ----
def _infer_mysql_types_from_sample(headers: list[str], text_data: str, sample_rows: int = 2000) -> list[str]:
    """
    headers/텍스트 CSV 샘플을 보고 MySQL 컬럼 타입 배열을 반환.
    - 숫자(정수/소수) → BIGINT 또는 DECIMAL(38,6)
    - 날짜(YYYY-MM-DD 형태 위주) → DATE
    - 그 외 문자열 → 길이에 따라 VARCHAR(16/32/64/128/191/255) 또는 TEXT
    """
    reader = csv.reader(io.StringIO(text_data))
    next(reader, None)  # header skip

    maxlens = [0]*len(headers)
    numcnt = [0]*len(headers)
    datecnt= [0]*len(headers)
    nonmiss=[0]*len(headers)
    decimal_seen=[False]*len(headers)

    # 간단한 날짜 패턴(YYYY-MM-DD) 우선
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    num_re  = re.compile(r"^-?\d+(?:\.\d+)?$")

    for i, row in enumerate(reader):
        if i >= sample_rows:
            break
        row = (row + [""] * len(headers))[:len(headers)]
        for j, cell in enumerate(row):
            if cell is None:
                continue
            s = str(cell).strip()
            if s == "":
                continue
            nonmiss[j] += 1
            maxlens[j] = max(maxlens[j], len(s))
            if num_re.fullmatch(s):
                numcnt[j] += 1
                if "." in s:
                    decimal_seen[j] = True
            if date_re.fullmatch(s):
                datecnt[j] += 1

    types: list[str] = []
    for j in range(len(headers)):
        n = nonmiss[j] or 1
        num_ratio  = numcnt[j] / n
        date_ratio = datecnt[j] / n
        if date_ratio >= 0.9:
            types.append("DATE")
        elif num_ratio >= 0.9:
            types.append("DECIMAL(38,6)" if decimal_seen[j] else "BIGINT")
        else:
            L = maxlens[j]
            if L <= 16:    types.append("VARCHAR(16)")
            elif L <= 32:  types.append("VARCHAR(32)")
            elif L <= 64:  types.append("VARCHAR(64)")
            elif L <= 128: types.append("VARCHAR(128)")
            elif L <= 191: types.append("VARCHAR(191)")
            elif L <= 255: types.append("VARCHAR(255)")
            else:          types.append("TEXT")
    return types


def _approx_row_size(mysql_types: list[str]) -> int:
    """
    InnoDB 대략 행 크기 추정치 (오버헤드 단순화).
    - VARCHAR(N): N + (N<=255 ? 1 : 2)
    - TEXT류: 20 (off-page pointer라고 생각)
    - BIGINT: 8, DECIMAL: 16, DATE: 3
    """
    size = 0
    for t in mysql_types:
        u = t.upper()
        if u.startswith("VARCHAR("):
            m = re.search(r"\((\d+)\)", u)
            n = int(m.group(1)) if m else 255
            size += n + (1 if n <= 255 else 2)
        elif u.startswith("BIGINT"):
            size += 8
        elif u.startswith("DECIMAL"):
            size += 16
        elif u == "DATE":
            size += 3
        elif "TEXT" in u:
            size += 20
        else:
            size += 8  # 기타 여유치
    return size


def _shrink_types_to_fit(mysql_types: list[str], limit: int = 65000) -> list[str]:
    """
    행 크기가 limit를 넘으면 가장 큰 VARCHAR부터 TEXT로 강등하며 줄인다.
    """
    def largest_varchar_idx(types):
        idx, maxn = -1, -1
        for i, t in enumerate(types):
            m = re.match(r"(?i)varchar\((\d+)\)", t)
            if m:
                n = int(m.group(1))
                if n > maxn:
                    maxn, idx = n, i
        return idx

    types = mysql_types[:]
    while _approx_row_size(types) > limit:
        i = largest_varchar_idx(types)
        if i == -1:
            break  # 더 줄일 VARCHAR가 없음
        types[i] = "TEXT"
    return types

# [MOD] 타입 배열 지원 + ROW_FORMAT=DYNAMIC
def _build_ddl_from_headers(table_name: str, headers: list[str], col_types: Optional[list[str]] = None) -> str:
    t = _escape_mysql_identifier(table_name)
    if col_types is None:
        col_types = ["VARCHAR(255)"] * len(headers)
    cols = [f"  {_escape_mysql_identifier(h)} {col_types[i]} NULL" for i, h in enumerate(headers)]
    # DYNAMIC 로우포맷: 긴 가변길이 문자열 off-page 저장에 유리
    return (
        f"CREATE TABLE {t} (\n" + ",\n".join(cols) +
        "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;"
    )

def _humanize_sql_error(e: Exception) -> str:
    msg = str(e)
    try:
        orig = getattr(e, "orig", None)
        if orig:
            msg = str(orig)
    except Exception:
        pass
    msg = re.sub(r"\(Background on this error.*", "", msg).strip()

    # 행 크기 초과 요약 추가
    if re.search(r"Row size too large", msg, re.IGNORECASE):
        return ("❌ 행 크기가 너무 큽니다(> 65535 bytes). "
                "컬럼 수가 많거나 VARCHAR 길이가 큽니다. 자동으로 TEXT로 조정하거나 길이를 줄이세요.")

    m = re.search(r"Incorrect column name '([^']+)'", msg)
    if m:
        bad = m.group(1)
        return f"❌ 잘못된 컬럼명: '{bad}' · 공백/빈 값/제어문자/따옴표를 제거하거나 헤더를 수정하세요."

    return f"DB 오류: {msg}"






load_dotenv()  # .env 읽기

DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "data_platform")
DB_URL  = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
SERVER_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/?charset=utf8mb4"  # DB명 없이 접속

ENGINE = None
STARTUP_OK = False

# ---------------------------
# Lifespan: DB/스키마 준비
# ---------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global STARTUP_OK, ENGINE
    try:
        # 1) DB 없으면 생성
        tmp_engine = create_engine(SERVER_URL, pool_pre_ping=True)
        with tmp_engine.begin() as conn:
            conn.execute(text(f"""
                CREATE DATABASE IF NOT EXISTS `{DB_NAME}`
                CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            """))
        tmp_engine.dispose()

        # 2) 메인 엔진
        ENGINE = create_engine(DB_URL, pool_pre_ping=True)

        # 3) 메타 테이블(옵션)
        with ENGINE.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS `uploads_meta` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(128) NOT NULL,
                is_forecastable BOOLEAN DEFAULT FALSE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
            conn.execute(text("""
                ALTER TABLE uploads_meta ADD UNIQUE KEY uq_table_name (table_name);
        """))

        STARTUP_OK = True
        print("✅ startup: DB 및 uploads_meta 테이블 준비 완료")
    except Exception as e:
        print(f"❌ startup: 초기화 실패 → {e}")

    yield
    print("👋 shutdown")

# ---------------------------
# App & CORS
# ---------------------------
app = FastAPI(
    title="CSV Ingest (Schema-Aware, Auto-Merge) API",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

# ---------------------------
# In-memory cache
# ---------------------------
default_dir = os.path.join(pathlib.Path.home(), ".smartinv_uploads")

UPLOAD_DIR = os.getenv("UPLOAD_DIR", default_dir)
os.makedirs(UPLOAD_DIR, exist_ok=True)

class UploadMeta(BaseModel):
    file_id: str
    filename: str
    headers: List[str]
    header_signature: str
    header_hash: str
    num_columns: int
    uploaded_at: str

UPLOADS: Dict[str, UploadMeta] = {}
DDL_LOG: Dict[str, str] = {}

# ---------------------------
# Helpers
# ---------------------------
PREFERRED_ENCODINGS = [
    "utf-8-sig",
    "utf-8",
    "cp949",
    "euc-kr",
    "latin-1",  # fallback
]

def _decode_bytes(data: bytes) -> str:
    for enc in PREFERRED_ENCODINGS:
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")

# ---------------------------
# 시계열 데이터 감지 헬퍼
# ---------------------------
def _detect_timeseries_like(headers: list[str], text_data: str, col_types: list[str]) -> bool:
    """
    시계열 / 수요예측용 데이터인지 판정 (패널형도 통과시킴)
    규칙 순서:
    1) 시점 컬럼(Date, DateTime, 날짜 계열) 있는지
    2) 시점 값이 실제 날짜/시간으로 파싱되는지
    3) 같은 날짜에 여러 행이 있어도, '제품/품목/창고' 같은 키 컬럼이 있으면 허용
    4) 'T+1', 'T+2', 'T+7', '예정 수주량', '예상 수주량' 같은 horizon 컬럼이 있으면 강하게 True
    5) 숫자형 피처가 2개 이상인지
    """
    import csv, io, re
    import pandas as pd
    import numpy as np

    if any("datetime" in h.lower() for h in headers) and any("product" in h.lower() for h in headers):
        return True


    # 0) 헤더 전처리
    raw_headers = headers
    lower_headers = [h.strip().lower() for h in raw_headers]

    # 1) 날짜/시간 후보 찾기
    date_keywords = ["date", "datetime", "time", "day", "날짜", "일자", "기준일", "거래일", "영업일"]
    date_cols = [h for h in raw_headers if any(k in h.strip().lower() for k in date_keywords)]
    if not date_cols:
        # 날짜가 아예 없으면 시계열로 안 본다
        return False

    # 2) CSV -> DF
    reader = csv.reader(io.StringIO(text_data))
    rows = list(reader)
    if len(rows) < 2:
        return False
    df = pd.DataFrame(rows[1:], columns=raw_headers)
    if df.empty:
        return False

    # 3) 날짜 컬럼 실제 파싱 비율 체크
    parsed_date_col = None
    for c in date_cols:
        s = pd.to_datetime(df[c].astype(str).str.strip(), errors="coerce", format=None)
        valid_ratio = s.notna().mean()
        if valid_ratio >= 0.6:     # 60% 이상만 날짜로 읽혀도 인정
            parsed_date_col = c
            df["_parsed_dt_"] = s
            break
    if parsed_date_col is None:
        return False

    # 4) == 핵심 추가 ==
    #    supply_chain 데이터처럼 같은 날짜에 품목/창고 단위로 여러 행이 있는 경우:
    #    - 품목/제품/sku/warehouse 같은 키가 있으면 이것도 시계열로 본다.
    key_like_keywords = ["product", "item", "sku", "material", "warehouse", "location", "store", "shop", "code"]
    has_entity_key = any(
        any(k in h.strip().lower() for k in key_like_keywords)
        for h in raw_headers
    )

    # 날짜가 있긴 한데, 하루에 행이 너무 많으면 패널로 의심
    per_day_counts = None
    if "_parsed_dt_" in df.columns:
        per_day_counts = df.groupby(df["_parsed_dt_"].dt.date).size()
    is_panel_shape = per_day_counts is not None and per_day_counts.mean() > 1.5

    # 5) horizon / forecast 패턴 감지 (이게 있으면 거의 100% 예측용)
    horizon_patterns = [
        r"t\+\d+",
        r"\bforecast\b",
        r"\b예상\s*수주량",
        r"\b예정\s*수주량",
        r"\bprediction",
    ]
    hdr_join = " || ".join(lower_headers)
    has_horizon = any(re.search(pat, hdr_join, flags=re.IGNORECASE) for pat in horizon_patterns)

    if has_horizon:
        # 이건 네 supply_chain_data가 여기에 걸린다
        return True

    # 6) 일반적인 시간 순서성도 한 번은 본다 (단, 패널이면 이 체크는 스킵)
    if not is_panel_shape:
        dt_sorted = df["_parsed_dt_"].dropna().sort_values()
        diffs = dt_sorted.diff().dropna().dt.total_seconds()
        if len(diffs) == 0:
            return False
        # 간격 너무 들쭉날쭉하면 시계열로 안 본다
        mean_gap = np.mean(diffs)
        std_gap = np.std(diffs)
        if mean_gap > 0 and (std_gap / mean_gap) > 5.0:
            # 들쭉날쭉 → 로그성 데이터일 수 있음
            return False

    # 7) 숫자형 피처 개수 (수요예측/시계열이면 보통 2개 이상)
    num_like_cnt = sum(1 for t in col_types if any(x in t.lower() for x in ["int", "decimal", "float"]))
    if num_like_cnt < 1 and not has_horizon:
        # 숫자도 없고 horizon도 없으면 그냥 일반 테이블로
        return False

    # 8) 패널 + 날짜 + 엔티티키 있으면 시계열로 본다
    if is_panel_shape and has_entity_key:
        return True

    # 9) LLM 보정 (있을 때만)
    if "llm_advise_schema" in globals():
        try:
            ans = llm_advise_schema(raw_headers, col_types, [], ["demand", "sales", "quantity"])
            if isinstance(ans, dict):
                if any("time" in str(v).lower() for v in ans.values()):
                    return True
        except Exception:
            pass

    # 위 조건 다 통과했으면 True
    return True

def _detect_forecastable_like(headers: list[str], text_data: str, col_types: list[str]) -> bool:
    """
    '이 CSV로 수요예측(판매량/출고량/수주량/출고량 등)을 돌릴 수 있냐?' 판정용.
    - 먼저 '인사/급여/조직' 같은 비예측 도메인은 바로 컷
    - 그 다음에 '날짜 + 수량/매출/주문/출고 계열 타깃' 있는지 본다
    - 마지막으로 패널/단일시계열 형태가 되는지 본다
    """
    import csv, io, re
    import pandas as pd
    import numpy as np

    raw_headers = headers
    lower_headers = [h.strip().lower() for h in raw_headers]

    # 0. 완전 비즈니스 도메인 필터 (HR/급여/조직 관리) → 이런 건 수요예측 아니라고 봐야 함
    hr_like_keywords = [
        "사원", "사번", "직원", "employee", "emp_", "staff", "인사", "hr",
        "dept", "department", "부서", "팀명", "team", "position", "직급",
        "급여", "급여액", "salary", "pay", "wage", "연봉", "월급", "시급",
        "성과", "평가", "입사", "입사일", "퇴사", "퇴사일",
        "주민", "address", "전화", "tel", "휴대폰", "email"
    ]
    # 급여/직원 단어가 하나라도 있는데, 아래의 진짜 수요 타깃 단어가 하나도 없으면 바로 False
    demand_like_keywords = [
        "수요", "demand", "판매", "sales", "출고", "shipment", "ship_qty",
        "발주", "order", "주문", "forecast", "예측", "수주", "납품",
        "qty", "quantity", "수량", "매출", "revenue", "consumption",
        "재고", "stock", "inventory"
    ]
    has_hr_word = any(any(k in h for k in hr_like_keywords) for h in lower_headers)
    has_demand_word = any(any(k in h for k in demand_like_keywords) for h in lower_headers)
    if has_hr_word and not has_demand_word:
        # "직원급여.xlsx" 같은 건 여기서 막힌다
        return False

    # 1. CSV -> DataFrame 으로 잠깐 읽어서 실제 값 봄
    csv_buf = io.StringIO(text_data)
    reader = csv.reader(csv_buf)
    rows = list(reader)

    if not rows:
        return False

    header_row = rows[0]
    data_rows = rows[1:]

    # 데이터가 너무 없으면 예측 의미 X
    if len(data_rows) < 3:
        return False

    # pandas로 한 번 더
    df = pd.DataFrame(data_rows, columns=header_row)

    # 2. 날짜 컬럼 찾기
    date_candidates = []
    date_name_keywords = [
        "date", "day", "날짜", "일자", "기준일", "base_date",
        "dt", "ym", "yyyymm", "year", "month", "week"
    ]
    for col in df.columns:
        col_l = col.strip().lower()
        if any(k in col_l for k in date_name_keywords):
            date_candidates.append(col)

    # 이름으로 못 찾았으면 실제 값으로 찾기
    if not date_candidates:
        for col in df.columns:
            try:
                parsed = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
            except Exception:
                continue
            if parsed.notna().sum() >= max(3, int(len(df) * 0.3)):
                date_candidates.append(col)

    if not date_candidates:
        # 날짜 하나도 못 찾으면 예측 데이터로 보기 어렵다
        return False

    # 3. 타깃 컬럼(수량/매출/주문/출고/재고) 찾기
    qty_name_keywords = [
        "qty", "quantity", "수량", "판매", "sales", "출고", "shipment",
        "order_qty", "주문", "발주", "발주량", "order_amount",
        "demand", "forecast", "예측", "수요",
        "stock", "inventory", "onhand", "재고"
    ]
    num_like_cnt = 0
    has_qty_header = False
    for i, col in enumerate(df.columns):
        col_l = col.strip().lower()

        # 이름이 수요/수량 계열이면 우선 후보
        if any(k in col_l for k in qty_name_keywords):
            has_qty_header = True
            continue

        # 이름은 애매한데 숫자형으로 보이면 후보
        if i < len(col_types):
            if col_types[i].lower().startswith(("int", "bigint", "decimal", "float", "double")):
                num_like_cnt += 1

    # 진짜로 target 컬럼이 하나도 없으면 컷
    if not has_qty_header and num_like_cnt == 0:
        return False

    # 4. 엔티티 키(품목/상품/코드/창고) 있는지
    entity_keywords = [
        "item", "item_code", "item_cd", "product", "product_code", "prod_cd",
        "sku", "품목", "품번", "품목코드", "모델", "model",
        "고객", "customer", "거래처", "account",
        "창고", "wh", "warehouse", "store", "지점", "branch", "매장"
    ]
    has_entity_key = any(any(k in h for k in entity_keywords) for h in lower_headers)

    # 5. 실제 날짜 파싱해서 간격 좀 본다
    # 가장 첫 번째 날짜 후보만 본다 (대부분 1개)
    dtcol = date_candidates[0]
    parsed_dt = pd.to_datetime(df[dtcol], errors="coerce", infer_datetime_format=True)
    if parsed_dt.notna().sum() < 3:
        return False
    df["_parsed_dt_"] = parsed_dt

    # 6. 데이터가 "직원 단위"로 보이는지 한 번 더 필터
    # 예: 사번/직원명/부서명 + 날짜 + 숫자(급여) → 이건 위에서 한 번 걸렀지만
    # 그래도 date + salary 가 남을 수 있으니까 한 번 더
    salary_like = ["salary", "급여", "pay", "wage", "연봉", "월급"]
    if any(any(k in h for k in salary_like) for h in lower_headers) and not has_qty_header:
        return False

    # 7. 패널 형태인지(같은 날짜에 여러 행) 확인
    # 날짜 + 엔티티 있으면 패널로 보고 OK
    grp = df.groupby("_parsed_dt_").size()
    is_panel_shape = bool((grp > 1).any())

    # 8. 날짜 간격 너무 들쭉날쭉한 이벤트 로그는 제외
    dt_sorted = df["_parsed_dt_"].dropna().sort_values()
    diffs = dt_sorted.diff().dropna().dt.total_seconds()
    if len(diffs) >= 2:
        mean_gap = float(np.mean(diffs))
        std_gap = float(np.std(diffs))
        # gap이 너무 랜덤이면 로그성 → 예측 적합도 낮음
        if mean_gap > 0 and (std_gap / mean_gap) > 8.0:
            return False

    # --- 최종 판단 ---
    # 1) 날짜 있고
    # 2) 수량/매출/주문/재고 계열 target 있고
    # 3) (패널+엔티티)거나 단일시계열이면 OK
    if is_panel_shape:
        if has_entity_key and (has_qty_header or num_like_cnt >= 1):
            return True
        # 엔티티가 없는데 같은 날짜에 여러 건이면 로그에 가까우니 보수적으로 False
        return False
    else:
        # 단일 시계열: 날짜 한 줄씩 + 타깃 하나
        if has_qty_header or num_like_cnt == 1:
            return True

    return False




def _find_first_record_bytes(data: bytes) -> bytes:
    in_quote = False
    i = 0
    while i < len(data):
        b = data[i]
        if b == 0x22:  # "
            if in_quote and i + 1 < len(data) and data[i+1] == 0x22:
                i += 2
                continue
            in_quote = not in_quote
            i += 1
            continue
        if (b in (0x0A, 0x0D)) and not in_quote:
            return data[:i]
        i += 1
    return data

def _parse_headers_from_first_line(first_line_text: str) -> List[str]:
    s = first_line_text.replace("\r", "").replace("\n", "")
    reader = csv.reader([s])
    try:
        headers = next(reader, [])
    except Exception:
        headers = []
    return headers

def _assert_csv_filename(filename: str):
    if not filename or "." not in filename or filename.split(".")[-1].lower() != "csv":
        raise HTTPException(status_code=400, detail="CSV 파일이 아닙니다(.csv 확장자 필요).")

def _escape_mysql_identifier(name: str) -> str:
    return f"`{name.replace('`','``')}`"

def _sanitize_table_name(filename: str) -> str:
    # 확장자 제거 → 비허용문자 '_' 치환 → 길이 제한 → 비어있으면 보정
    name = os.path.splitext(filename)[0]
    name = re.sub(r"[^0-9A-Za-z가-힣_]+", "_", name).strip("_")
    if not name:
        name = "table_" + uuid.uuid4().hex[:8]
    if len(name) > 64:
        name = name[:64]
    return name

def _get_existing_table_schema(conn, db_name: str, table_name: str):
    rows = conn.execute(text("""
        SELECT column_name, column_type, is_nullable, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = :db AND table_name = :tbl
        ORDER BY ordinal_position ASC
    """), {"db": db_name, "tbl": table_name}).fetchall()
    return [
        {"name": r[0], "column_type": str(r[1]).lower(), "nullable": (str(r[2]).lower()=="yes")}
        for r in rows
    ]


def _same_schema(existing: List[Dict[str, Any]], headers: List[str]) -> bool:
    if len(existing) != len(headers):
        return False
    for e, h in zip(existing, headers):
        if e["name"] != h:
            return False
        # 우리가 만든 테이블 기준: varchar(255) / nullable
        if ("varchar" not in e["column_type"]) or (e["nullable"] is not True):
            return False
    return True



# [ADD] 헤더 정제기: 공백/제어문자 제거, 완전 빈 값 보정, 충돌 시 __2, __3 부여
def _sanitize_headers_for_mysql(headers: list[str]) -> tuple[list[str], list[tuple[str, str]], list[str]]:
    """
    returns: (cleaned_headers, mapping[(original, cleaned)], warnings)
    """
    cleaned: list[str] = []
    mapping: list[tuple[str, str]] = []
    warnings: list[str] = []

    # 1) 1차 정제: trim, 제어문자 제거, 내부 다중 공백 접기
    for i, h in enumerate(headers):
        orig = str(h or "")
        s = orig.strip()
        # 제어문자 제거
        s = re.sub(r"[\x00-\x1f\x7f]", "", s)
        # 내부 다중 공백 -> 하나로
        s = re.sub(r"\s+", " ", s)

        # 완전 빈 값이면 자동 이름 부여
        if s == "":
            s = f"col_{i+1}"
            warnings.append(f"빈 컬럼명 감지 → '{orig}' 를 '{s}' 로 대체")

        mapping.append((orig, s))
        cleaned.append(s)

    # 2) 중복 해결: 동일 이름 있으면 __2, __3...
    seen: dict[str, int] = {}
    for i, s in enumerate(cleaned):
        base = s
        n = seen.get(base, 0)
        if n == 0:
            seen[base] = 1
            continue
        # 이미 존재 → 접미사 증가
        while True:
            n += 1
            cand = f"{base}__{n}"
            if cand not in seen:
                cleaned[i] = cand
                seen[cand] = 1
                warnings.append(f"중복 컬럼명 충돌 → '{base}'를 '{cand}' 로 변경")
                break
        seen[base] = n

    return cleaned, mapping, warnings

# ---------------------------
# Schemas (IO)
# ---------------------------
class SchemaCol(BaseModel):
    name: str
    mysql_type: str = Field(default="VARCHAR(255)")
    nullable: bool = Field(default=True)

class CreateTableRequest(BaseModel):
    table_name: str
    schema: List[SchemaCol]

class CreateTableResponse(BaseModel):
    ddl: str
    dry_run: bool = True

class StatsRequest(BaseModel):
    table_name: str

class StatsResponse(BaseModel):
    table_name: str
    row_count: int
    column_count: int

class UploadResponse(BaseModel):
    file_id: str
    filename: str
    headers: List[str]
    header_signature: str
    header_hash: str
    num_columns: int
    table_name: Optional[str] = None
    table_action: Optional[str] = None   # created|merged|replaced|error:...
    staged_rows: Optional[int] = None
    merged_rows: Optional[int] = None
    header_translation: Optional[Dict[str, str]] = None  # 원본 헤더 -> 최종 헤더
    header_warnings: List[str] = []                      # 정제 경고들
    is_forecastable: Optional[bool] = None

# ---------------------------
# Health
# ---------------------------
@app.get("/_health")
def health():
    return {"engine": bool(ENGINE), "startup_ok": STARTUP_OK}

# ---------------------------
# UPLOAD → 자동 생성/병합/교체 + 합집합 적재
# ---------------------------
@app.post("/upload", response_model=UploadResponse)
async def upload_csv(file: UploadFile = File(...)):
    _assert_csv_filename(file.filename)

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="빈 파일입니다.")

    # 헤더 파싱
    first_line_bytes = _find_first_record_bytes(raw)
    if not first_line_bytes:
        raise HTTPException(status_code=400, detail="헤더 줄을 찾을 수 없습니다.")
    header_signature_text = _decode_bytes(first_line_bytes).rstrip("\r\n")
    headers = _parse_headers_from_first_line(header_signature_text)
    
    if not headers:
        raise HTTPException(status_code=400, detail="헤더가 비어있습니다.")

    headers_original = list(headers)
    
    # ✅ 헤더 정제 적용 (trim, 공백 접기, 빈 컬럼 자동명, 중복 충돌 해결)
    clean_headers, mapping, hdr_warnings = _sanitize_headers_for_mysql(headers)
    headers = clean_headers

    # 1) 원본→정제 & 기본 원본→최종
    original_to_clean = {orig: clean for (orig, clean) in mapping}
    original_to_final = original_to_clean.copy()

    # 2) (옵션) 헤더 번역
    want_translate_hdr = os.getenv("TRANSLATE_HEADERS", "false").lower() in ("1","true","yes")
    all_ascii = all(_is_ascii_identifier(h) for h in headers)

    if want_translate_hdr and not all_ascii and _translate_table_name is not None:
        translated = []
        for h in headers:
            try:
                en = _translate_table_name(h)
                translated.append(en or h)
            except Exception as e:
                logging.warning("헤더 변환 실패(%r) → 원본 사용: %s", e, h)
                translated.append(h)
        headers = _ensure_unique_names(translated)

        # 정제→최종
        clean_to_final = dict(zip(clean_headers, headers))
        # 원본→최종 = 합성
        original_to_final = {
            orig: clean_to_final.get(original_to_clean.get(orig, orig), original_to_clean.get(orig, orig))
            for orig in headers_original
        }
    else:
        logging.info("Header translation skipped (want=%s, ascii=%s, has_llm=%s)",
                    want_translate_hdr, all_ascii, _translate_table_name is not None)




    header_hash = hashlib.sha256(first_line_bytes).hexdigest()

    # 원본 저장
    file_id = str(uuid.uuid4())
    save_path = os.path.join(UPLOAD_DIR, f"{file_id}.csv")
    with open(save_path, "wb") as f:
        f.write(raw)

    meta = UploadMeta(
        file_id=file_id,
        filename=file.filename,
        headers=headers,
        header_signature=header_signature_text,
        header_hash=header_hash,
        num_columns=len(headers),
        uploaded_at=datetime.now(timezone.utc).isoformat()
    )
    UPLOADS[file_id] = meta

    text_data = _decode_bytes(raw)
    col_types = _infer_mysql_types_from_sample(headers, text_data, sample_rows=2000)
    col_types = _shrink_types_to_fit(col_types, limit=65000)

    # --- 수요예측 가능한 데이터 여부 ---
    is_fc_clean = _detect_forecastable_like(clean_headers, text_data, col_types)
    is_fc_final = _detect_forecastable_like(headers, text_data, col_types)
    is_forecastable = is_fc_clean or is_fc_final


    original_base = os.path.splitext(file.filename)[0]
    table_name = _sanitize_table_name(file.filename)  # 기본 폴백

    # ✅ 파일명이 이미 ASCII면 번역 스킵
    original_base = os.path.splitext(file.filename)[0]
    table_name = _sanitize_table_name(file.filename)  # 기본 폴백

    want_translate_tbl = os.getenv("TRANSLATE_TABLE_NAME", "false").lower() in ("1","true","yes")
    if want_translate_tbl and _translate_table_name is not None and not _is_ascii_identifier(original_base):
        try:
            ai_name = _translate_table_name(original_base)
            if ai_name:
                table_name = ai_name   # translator가 snake_case/ASCII/길이 제한 보장
            logging.info("테이블명 번역 적용: %s -> %s", original_base, table_name)
        except Exception as e:
            logging.warning("테이블명 번역 실패(%s) → 폴백 사용: %s", e, table_name)
    else:
        logging.info("테이블명 번역 스킵(want=%s, ascii=%s, has_llm=%s)", 
                    want_translate_tbl, _is_ascii_identifier(original_base), _translate_table_name is not None)


    table_action: Optional[str] = None
    staged_rows = 0
    merged_rows = 0

    if ENGINE is not None:
        try:
            with ENGINE.begin() as conn:
                # 1) 테이블 존재 여부
                exists = conn.execute(text("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema=:db AND table_name=:tbl
                """), {"db": DB_NAME, "tbl": table_name}).scalar() or 0

                # 2) (존재하면) 드롭 – 타입이 바뀔 수 있으니 항상 drop-create로 일관화
                if exists:
                    conn.execute(text(f"DROP TABLE {_escape_mysql_identifier(table_name)}"))

                # 3) 추론 타입(col_types)로 메인 테이블 생성
                ddl = _build_ddl_from_headers(table_name, headers, col_types)
                conn.execute(text(ddl))
                table_action = "created" if not exists else "replaced"

                # 4) 스테이징 테이블도 동일 타입으로 생성
                reader = csv.reader(io.StringIO(text_data))  # text_data는 함수 상단에서 이미 만든 걸 사용
                next(reader, None)  # 헤더 스킵
                stg = f"_stg_{uuid.uuid4().hex[:8]}"
                stg_ddl = _build_ddl_from_headers(stg, headers, col_types)
                conn.execute(text(stg_ddl))

                cols_esc = ", ".join(_escape_mysql_identifier(h) for h in headers)
                ph = ", ".join([f":c{i}" for i in range(len(headers))])
                ins_stg = text(f"INSERT INTO {_escape_mysql_identifier(stg)} ({cols_esc}) VALUES ({ph})")

                # 5) 배치 적재
                batch: List[Dict[str, Any]] = []
                for row in reader:
                    row = (row + [""] * len(headers))[:len(headers)]
                    vals = {f"c{i}": (v if v != "" else None) for i, v in enumerate(row)}
                    batch.append(vals)
                    if len(batch) >= 5000:
                        conn.execute(ins_stg, batch)
                        staged_rows += len(batch)
                        batch.clear()
                if batch:
                    conn.execute(ins_stg, batch)
                    staged_rows += len(batch)
                    batch.clear()

                # 6) 합집합 머지
                on_clause = " AND ".join(
                    [f"t.{_escape_mysql_identifier(h)} <=> s.{_escape_mysql_identifier(h)}" for h in headers]
                )
                merge_sql = text(f"""
                    INSERT INTO {_escape_mysql_identifier(table_name)} ({cols_esc})
                    SELECT {cols_esc}
                    FROM {_escape_mysql_identifier(stg)} s
                    WHERE NOT EXISTS (
                    SELECT 1 FROM {_escape_mysql_identifier(table_name)} t
                    WHERE {on_clause}
                    )
                """)
                res = conn.execute(merge_sql)
                merged_rows = getattr(res, "rowcount", None) or 0

                # 7) 스테이징 테이블 제거
                conn.execute(text(f"DROP TABLE {_escape_mysql_identifier(stg)}"))

                # 시계열 메타정보 저장
                conn.execute(
                    text("""
                        INSERT INTO uploads_meta (table_name, is_forecastable)
                        VALUES (:tname, :is_fc)
                        ON DUPLICATE KEY UPDATE is_forecastable = VALUES(is_forecastable)
                    """),
                    {"tname": table_name, "is_fc": 1 if is_forecastable else 0},
                )

        except Exception as e:
            table_action = f"error: {_humanize_sql_error(e)}"


    return UploadResponse(
        file_id=file_id,
        filename=file.filename,
        headers=headers,
        header_signature=header_signature_text,
        header_hash=header_hash,
        num_columns=len(headers),
        table_name=table_name,
        table_action=table_action,
        staged_rows=staged_rows,
        merged_rows=merged_rows,
        header_translation=original_to_final,
        header_warnings=hdr_warnings,
        is_forecastable=is_forecastable,
    )

# ---------------------------
# (선택) DDL 프리뷰/실행
# ---------------------------
class InferSchemaResponse(BaseModel):
    schema: List[SchemaCol]
    notes: List[str] = Field(default_factory=lambda: ["mock: echo-only"])

class InferSchemaRequest(BaseModel):
    file_id: str
    headers: List[str]

@app.post("/infer_schema", response_model=InferSchemaResponse)
def infer_schema(req: InferSchemaRequest):
    up = UPLOADS.get(req.file_id)
    if not up:
        raise HTTPException(status_code=400, detail="unknown file_id")
    if req.headers != up.headers:
        raise HTTPException(status_code=400, detail="headers mismatch: /upload 응답 그대로 보내세요.")
    return InferSchemaResponse(schema=[SchemaCol(name=h) for h in req.headers])

@app.post("/create_table", response_model=CreateTableResponse)
def create_table(req: CreateTableRequest, exec: bool = Query(False, alias="exec")):
    if not req.table_name or not req.table_name.strip():
        raise HTTPException(status_code=400, detail="table_name이 비어있습니다.")
    if not req.schema:
        raise HTTPException(status_code=400, detail="schema가 비어있습니다.")
    t = _escape_mysql_identifier(req.table_name.strip())
    cols = []
    for c in req.schema:
        cols.append(f"  {_escape_mysql_identifier(c.name)} {c.mysql_type} " + ("NULL" if c.nullable else "NOT NULL"))
    ddl = f"CREATE TABLE {t} (\n" + ",\n".join(cols) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    DDL_LOG[req.table_name] = ddl
    if not exec:
        return CreateTableResponse(ddl=ddl, dry_run=True)
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB 엔진 미초기화")
    try:
        with ENGINE.begin() as conn:
            conn.execute(text(ddl))
        return CreateTableResponse(ddl=ddl, dry_run=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"DDL 실행 실패: {e}")

# ---------------------------
# Stats
# ---------------------------
@app.post("/stats", response_model=StatsResponse)
def stats(req: StatsRequest):
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB 엔진 미초기화")
    tname = req.table_name.strip()
    if not tname:
        raise HTTPException(status_code=400, detail="table_name required")
    t_esc = _escape_mysql_identifier(tname)
    try:
        with ENGINE.connect() as conn:
            row_count = conn.execute(text(f"SELECT COUNT(*) FROM {t_esc}")).scalar() or 0
            column_count = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema = :db AND table_name = :tbl
            """), {"db": DB_NAME, "tbl": tname}).scalar() or 0
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"stats failed: {e}")
    return StatsResponse(table_name=tname, row_count=row_count, column_count=column_count)

# ---------------------------
# Debug
# ---------------------------
@app.get("/_debug/uploads")
def list_uploads() -> Dict[str, Any]:
    return {fid: UPLOADS[fid].model_dump() for fid in UPLOADS}

@app.get("/_debug/ddl/{key}")
def get_logged_ddl(key: str) -> Dict[str, Any]:
    return {"key": key, "ddl": DDL_LOG.get(key)}

# --- ADD: DB 내 테이블 목록 조회 ---
@app.get("/tables")
def list_tables():
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB 엔진 미초기화: .env/DB 설정을 확인하세요.")
    try:
        with ENGINE.connect() as conn:
            rows = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :db
                ORDER BY table_name
            """), {"db": DB_NAME}).fetchall()
        return {"tables": [r[0] for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"list tables failed: {e}")
    
@app.get("/timeseries-tables")
def get_timeseries_tables():
    """
    uploads_meta에서 is_timeseries=1 인 테이블명만 돌려줌
    """
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB engine not ready")
    with ENGINE.begin() as conn:
        rows = conn.execute(text("""
            SELECT table_name
            FROM uploads_meta
            WHERE is_forecastable = 1
            ORDER BY id DESC
        """)).fetchall()
    return {"tables": [r[0] for r in rows]}

@app.get("/all-tables")
def list_all_tables():
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB engine not ready")
    with ENGINE.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :db
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """), {"db": DB_NAME}).fetchall()
    return {"tables": [r[0] for r in rows]}


class ColumnsRequest(BaseModel):
    table_name: str

@app.post("/table-columns")
def get_table_columns(req: ColumnsRequest):
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB engine not ready")

    tbl = req.table_name.strip()
    if not tbl:
        raise HTTPException(status_code=400, detail="table_name is required")

    with ENGINE.begin() as conn:
        rows = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :db AND table_name = :tbl
            ORDER BY ordinal_position
        """), {"db": DB_NAME, "tbl": tbl}).fetchall()

    return {"table_name": tbl, "columns": [r[0] for r in rows]}

@app.get("/table/{table_name}/preview")
def preview_table(table_name: str, limit: int = 100):
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB engine not initialized")
    t_esc = _escape_mysql_identifier(table_name.strip())
    try:
        with ENGINE.connect() as conn:
            cols = [r[0] for r in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :db AND table_name = :tbl "
                "ORDER BY ordinal_position"
            ), {"db": DB_NAME, "tbl": table_name}).fetchall()]
            rows = conn.execute(
                text(f"SELECT * FROM {t_esc} LIMIT :limit"), {"limit": limit}
            ).fetchall()
        return {"columns": cols, "rows": [list(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/table/{table_name}/download")
def download_table_csv(table_name: str):
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB engine not initialized")
    t_esc = _escape_mysql_identifier(table_name.strip())
    try:
        with ENGINE.connect() as conn:
            cols = [r[0] for r in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :db AND table_name = :tbl "
                "ORDER BY ordinal_position"
            ), {"db": DB_NAME, "tbl": table_name}).fetchall()]
            rows = conn.execute(text(f"SELECT * FROM {t_esc}")).fetchall()
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(cols)
        for r in rows: writer.writerow(r)
        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{table_name}.csv"'}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# === main.py ===
from pydantic import BaseModel
from typing import List, Any

class ReplaceRequest(BaseModel):
    table_name: str
    columns: List[str]
    rows: List[List[Any]]

class SaveAsRequest(BaseModel):
    src_table: str
    new_table: str
    columns: List[str]
    rows: List[List[Any]]

def _esc(name: str) -> str:
    # MySQL 예약어/기호 대응: 반드시 식별자 이스케이프
    if not name: raise ValueError("empty identifier")
    if "`" in name: raise ValueError("backtick in identifier")
    return f"`{name}`"

@app.post("/table/replace")
def replace_table(req: ReplaceRequest):
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB engine not initialized")
    t = _esc(req.table_name.strip())
    if not req.columns:
        raise HTTPException(status_code=400, detail="columns required")

    cols = [ _esc(c) for c in req.columns ]
    placeholders = ", ".join([f":c{i}" for i in range(len(cols))])
    col_list = ", ".join(cols)

    # 대용량 안전: 청크 단위 insert
    CHUNK = 1000

    try:
        with ENGINE.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {t}"))
            ins = text(f"INSERT INTO {t} ({col_list}) VALUES ({placeholders})")
            n = 0
            for i in range(0, len(req.rows), CHUNK):
                chunk = req.rows[i:i+CHUNK]
                params = []
                for r in chunk:
                    p = { f"c{j}": (r[j] if j < len(req.columns) else None) for j in range(len(req.columns)) }
                    params.append(p)
                if params:
                    conn.execute(ins, params)
                    n += len(params)
        return {"status": "ok", "inserted": n}
    except Exception as ex:
        short = _humanize_sql_error(ex)
        raise HTTPException(status_code=400, detail=f"replace failed: {short}")

@app.post("/table/save_as")
def save_as(req: SaveAsRequest):
    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB engine not initialized")
    src = _esc(req.src_table.strip())
    dst = _esc(req.new_table.strip())
    if not req.columns:
        raise HTTPException(status_code=400, detail="columns required")

    cols = [ _esc(c) for c in req.columns ]
    placeholders = ", ".join([f":c{i}" for i in range(len(cols))])
    col_list = ", ".join(cols)
    CHUNK = 1000

    try:
        with ENGINE.begin() as conn:
            # 스키마 복제
            exists = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = :db AND table_name = :tbl
            """), {"db": DB_NAME, "tbl": req.new_table}).scalar() or 0
            if exists:
                raise HTTPException(status_code=400, detail=f"save_as failed: 테이블 '{req.new_table}' 이(가) 이미 존재합니다.")
            conn.execute(text(f"CREATE TABLE {dst} LIKE {src}"))

            # 데이터 삽입
            ins = text(f"INSERT INTO {dst} ({col_list}) VALUES ({placeholders})")
            n = 0
            for i in range(0, len(req.rows), CHUNK):
                chunk = req.rows[i:i+CHUNK]
                params = []
                for r in chunk:
                    p = { f"c{j}": (r[j] if j < len(req.columns) else None) for j in range(len(req.columns)) }
                    params.append(p)
                if params:
                    conn.execute(ins, params)
                    n += len(params)
        return {"status": "ok", "table": req.new_table, "inserted": n}
    except Exception as ex:
        short = _humanize_sql_error(ex)
        raise HTTPException(status_code=400, detail=f"save_as failed: {short}")

class AutoTrainRequest(BaseModel):
    headers: list[str]
    rows: list[list]
    targets: list[str]
    horizon: int = 14
    use_llm: bool = True
    table_name: str | None = None

class AutoTrainResult(BaseModel):
    summary: str
    by_target: dict

def _is_datetime_series(series: pd.Series) -> bool:
     # 🔧 2) 같은 이름의 컬럼이 여러 개라서 DataFrame 이 들어오는 경우 방지
    import pandas as pd
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    try:
        pd.to_datetime(series, errors="raise")
        return True
    except Exception:
        return False

def _choose_time_col(df: pd.DataFrame):
    def _as_series(x):
        return x.iloc[:,0] if isinstance(x, pd.DataFrame) else x

    # 흔한 이름 우선
    for c in df.columns:
        lc = str(c).lower()
        if any(k in lc for k in ("date","dt","timestamp","time","날짜","일자")):
            if _is_datetime_series(_as_series(df[c])): 
                return c
    # 전체 스캔
    for c in df.columns:
        if _is_datetime_series(_as_series(df[c])): 
            return c
    return None

def _build_tabular_regressor(df: pd.DataFrame, ycol: str):
    X = df.drop(columns=[ycol])
    y = df[ycol].astype(float)

    num_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]
    cat_cols = [c for c in X.columns if c not in num_cols]

    pre = ColumnTransformer([
        ("num", SimpleImputer(strategy="median"), num_cols),
        ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                          ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat_cols)
    ])
    model = RandomForestRegressor(n_estimators=300, random_state=42, n_jobs=-1)
    pipe = Pipeline([("pre", pre), ("rf", model)])
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
    pipe.fit(Xtr, ytr)
    pred = pipe.predict(Xte)
    mae = float(mean_absolute_error(yte, pred))
    return pipe, {"mae": mae, "n_train": int(len(Xtr)), "n_test": int(len(Xte))}

def _build_ts_with_lags(df: pd.DataFrame, time_col: str, ycol: str, horizon: int):
    # 🔒 먼저 safety pick (혹시 상위에서 넘어온 두 이름이 또 변했을 때)
    tcol = _pick_existing_col(df, time_col) or time_col
    ycol2 = _pick_existing_col(df, ycol) or ycol
    if ycol2 not in df.columns:
        raise KeyError(f"target column not found: {ycol} / candidates={ [c for c in df.columns if str(c).startswith(f'{ycol}')] }")

    # 필요한 두 컬럼만 복사
    cols = [tcol, ycol2] if tcol != ycol2 else [tcol]
    use = df.loc[:, cols].copy()

    # 시간 컬럼 → Series 강제 + 변환
    s_time = use[tcol]
    if isinstance(s_time, pd.DataFrame):
        s_time = s_time.iloc[:, 0]
    use["__ts__"] = pd.to_datetime(s_time, errors="coerce")
    if tcol in use.columns:
        use = use.drop(columns=[tcol])
    tcol = "__ts__"

    # 타깃 → Series 강제 + 숫자화
    s_y = use[ycol2] if ycol2 in use.columns else df[ycol2]
    if isinstance(s_y, pd.DataFrame):
        s_y = s_y.iloc[:, 0]
    s_y = pd.to_numeric(s_y, errors="coerce")
    use[ycol2] = s_y

    use = use.dropna(subset=[tcol]).sort_values(tcol)

    # 랙 피처
    for L in (1, 2, 3, 7, 14):
        use[f"lag_{L}"] = use[ycol2].shift(L)

    use = use.dropna()
    X = use.drop(columns=[ycol2, tcol])
    y = use[ycol2].astype(float)

    tscv = TimeSeriesSplit(n_splits=5)
    last_mae = None
    model = RandomForestRegressor(n_estimators=400, random_state=42, n_jobs=-1)
    for tr_idx, te_idx in tscv.split(X):
        Xtr, Xte = X.iloc[tr_idx], X.iloc[te_idx]
        ytr, yte = y.iloc[tr_idx], y.iloc[te_idx]
        model.fit(Xtr, ytr)
        pred = model.predict(Xte)
        last_mae = float(mean_absolute_error(yte, pred))

    # 롤링 예측
    hist = use.tail(max(14, horizon)).copy()
    future = []
    cur = hist.iloc[-1].copy()
    for h in range(horizon):
        row = {k: cur.get(k, np.nan) for k in X.columns}
        yhat = float(model.predict(pd.DataFrame([row]))[0])
        for L in (14, 7, 3, 2, 1):
            key = f"lag_{L}"
            if key in cur.index:
                cur[key] = cur.get(f"lag_{L-1}", yhat if L == 1 else cur[key])
        future.append(yhat)

    return model, {"mae_cv": last_mae, "horizon": horizon, "forecast": future}

@app.post("/auto_train", response_model=AutoTrainResult)
def auto_train(req: AutoTrainRequest):
    # df 생성 직후 (이미 있던 코드에 추가/보강)
    df = pd.DataFrame(req.rows, columns=req.headers).copy()

    # (A) 중복 헤더 유니크화
    if df.columns.duplicated().any():
        seen = {}
        new_cols = []
        for c in df.columns:
            k = seen.get(c, 0)
            new_cols.append(c if k == 0 else f"{c}__{k+1}")
            seen[c] = k + 1
        df.columns = new_cols

    # (B) 숫자 변환 시도 (날짜처럼 생긴 건 건드리지 않음)
    for c in df.columns:
        try:
            df[c] = pd.to_numeric(df[c])
        except Exception:
            pass

    # (C) 시간 컬럼 선택 시에도 안전 픽커 사용
    raw_time_col = _choose_time_col(df)   # 기존 함수 호출
    time_col = _pick_existing_col(df, raw_time_col) if raw_time_col else None

    # 2) (선택) LLM에 스키마/샘플 전달해 유형 조언 받기
    llm_suggest = None
    if req.use_llm:
        samples = df.head(5).to_dict(orient="records")
        dtypes = {c:str(df[c].dtype) for c in df.columns}
        llm_suggest = llm_advise_schema(list(df.columns), dtypes, samples, req.targets)

    # 3) 시계열 time_col 후보
    time_col = _choose_time_col(df)

    results = {}
    for y in req.targets:
        # 🔒 타깃명 보정 (가장 먼저!)
        y_resolved = _pick_existing_col(df, y)
        if not y_resolved:
            results[y] = {"error": f"target '{y}' not found · available={list(df.columns)[:50]} ..."}
            continue

        # 숫자형 판단은 보정된 이름 기준
        y_is_numeric = (
            pd.api.types.is_numeric_dtype(df[y_resolved]) or
            pd.api.types.is_float_dtype(pd.to_numeric(df[y_resolved], errors="coerce"))
        )

        # LLM & 휴리스틱 결합
        want_ts = False
        if req.use_llm and llm_suggest and isinstance(llm_suggest, dict):
            v = str(llm_suggest.get(y, "")).lower()  # LLM은 사용자가 클릭한 원래 이름으로 답했을 수 있음
            want_ts = ("time" in v)

        if not want_ts and time_col and y_is_numeric:
            want_ts = True

        if want_ts and time_col:
            model, meta = _build_ts_with_lags(df, time_col, y_resolved, req.horizon)
            results[y] = {"mode": "time_series_lag_rf", **meta, "resolved": y_resolved}
        else:
            model, meta = _build_tabular_regressor(df, y_resolved)
            snap = model.predict(df.drop(columns=[y_resolved], errors="ignore").head(10))
            results[y] = {"mode": "tabular_rf", **meta, "sample_pred": [float(x) for x in snap], "resolved": y_resolved}

    # 4) 응답
    summary = f"targets={req.targets} · time_col={time_col} · llm_used={bool(llm_suggest)}"
    return AutoTrainResult(summary=summary, by_target=results)

from pydantic import BaseModel

class TrainFromTableReq(BaseModel):
    table_name: str
    target_cols: list[str]
    horizon: int = 14

class ForecastTrainRequest(BaseModel):
    table_name: str
    target_cols: list[str]
    time_col: str
    product_col: str
    horizon: int = 14

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import text
import pandas as pd
import numpy as np
from datetime import timedelta

# 이미 위에 ENGINE, DB_NAME 같은 거 있다고 가정
# time 컬럼 추측용
TIME_CANDIDATES = ["date", "dt", "day", "biz_date", "order_date", "forecast_date"]

class TrainFromTableReq(BaseModel):
    table_name: str
    target_cols: list[str] = []
    horizon: int = 14


@app.post("/train_from_table")
def train_from_table(req: ForecastTrainRequest):
    if ENGINE is None:
      raise HTTPException(status_code=500, detail="DB engine not ready")

    tbl = req.table_name.strip()
    if not tbl:
        raise HTTPException(status_code=400, detail="table_name required")

    tgt_cols = req.target_cols or []
    if not tgt_cols:
        raise HTTPException(status_code=400, detail="target_cols required")

    main_target = tgt_cols[0]   # 여러개 받았어도 1개만 그릴 거라 첫번째
    time_col = req.time_col.strip()
    product_col = req.product_col.strip()
    horizon = req.horizon or 14

    # 1) 테이블에서 필요한 컬럼만 읽기
    col_list = {time_col, product_col, main_target}
    cols_sql = ", ".join(f"`{c}`" for c in col_list)
    sql = f"SELECT {cols_sql} FROM `{tbl}`"
    df = pd.read_sql(sql, con=ENGINE)

    if df.empty:
        return {"forecast": None, "products": []}

    # 2) 컬럼 정리
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])
    df = df.sort_values(time_col)

    # 3) 제품코드 목록
    products = df[product_col].dropna().unique().tolist()

    out_series = {}
    for code in products:
        sub = df[df[product_col] == code].copy()
        if sub.empty:
            continue

        history = []
        for _, r in sub.iterrows():
            history.append({
                "date": r[time_col].strftime("%Y-%m-%d"),
                "value": float(r[main_target]) if pd.notna(r[main_target]) else None
            })

        last_date = sub[time_col].max()

        # ====== ✨ 예측 로직 개선 시작 ======
        # 1) 최근 N개로 이동평균
        WINDOW = 7  # 최근 7개 평균
        recent = sub[main_target].dropna().tail(WINDOW)
        if not recent.empty:
            base_val = float(recent.mean())   # 최근 평균
        else:
            # 데이터가 없으면 그냥 마지막 값
            base_val = float(sub[main_target].iloc[-1]) if pd.notna(sub[main_target].iloc[-1]) else 0.0

        # 2) 최근 값들로 아주 약한 추세(slope) 계산
        #    (마지막값 - 첫값) / (갯수-1) 으로 하루 증가량 비슷하게 추정
        slope = 0.0
        if len(recent) >= 2:
            start_v = float(recent.iloc[0])
            end_v = float(recent.iloc[-1])
            step = len(recent) - 1
            if step > 0:
                slope = (end_v - start_v) / step   # 하루당 변화량
        # 너무 요동치면 slope가 너무 커질 수 있으니 clamp
        MAX_SLOPE = base_val * 0.2 if base_val > 0 else 5  # base의 20%/day 또는 최대 5
        if slope > MAX_SLOPE: slope = MAX_SLOPE
        if slope < -MAX_SLOPE: slope = -MAX_SLOPE

        future = []
        for i in range(1, horizon + 1):
            fut_date = (last_date + pd.Timedelta(days=i)).strftime("%Y-%m-%d")
            # 3) 예측값 = 이동평균 + (기울기 * i)
            pred_val = base_val + slope * i
            # 음수 안 내려가게
            if pred_val < 0:
                pred_val = 0
            future.append({
                "date": fut_date,
                "value": float(round(pred_val, 2))
            })
        # ====== ✨ 예측 로직 개선 끝 ======


        out_series[str(code)] = {
            "product_code": str(code),
            "history": history,
            "future": future,
        }

    return {
        "table": tbl,
        "time_col": time_col,
        "product_col": product_col,
        "target": main_target,
        "horizon": horizon,
        "products": list(out_series.keys()),
        "series": out_series,
    }

# =====================================================
# ✅ /table-preview : 지정된 테이블의 일부 행 미리보기
# =====================================================
from fastapi import Body

@app.post("/table-preview")
def table_preview(
    body: dict = Body(...),
):
    """
    요청 JSON:
      { "table_name": "테이블명", "max_rows": 1000 }

    응답 JSON:
      { "headers": [...], "rows": [ {col:value,...}, ... ] }
    """
    table_name = body.get("table_name")
    max_rows = int(body.get("max_rows", 1000))

    if not table_name:
        raise HTTPException(status_code=400, detail="table_name is required")

    if ENGINE is None:
        raise HTTPException(status_code=500, detail="DB engine not initialized")

    try:
        with ENGINE.begin() as conn:
            # 컬럼 목록 가져오기
            cols_query = text(f"SHOW COLUMNS FROM `{table_name}`")
            cols = [r[0] for r in conn.execute(cols_query)]
            if not cols:
                raise HTTPException(status_code=404, detail="No columns found")

            # 데이터 미리보기
            preview_query = text(f"SELECT * FROM `{table_name}` LIMIT {max_rows}")
            rows = [dict(r._mapping) for r in conn.execute(preview_query)]

        return {
            "ok": True,
            "table_name": table_name,
            "headers": cols,
            "rows": rows,
            "row_count": len(rows),
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"DB query failed: {e}")
