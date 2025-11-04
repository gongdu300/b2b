import re
import time
import random
from typing import Optional

# 간단 LRU 느낌의 캐시 (용도: 동일 테이블명 반복 번역 방지)
# 실서비스면 functools.lru_cache나 외부 캐시(Redis) 사용 권장
_CACHE = {}
_CACHE_MAX = 512

# SQL 예약어(대표적) — 필요하면 확장
_SQL_RESERVED = {
    "select","from","where","join","table","index","group","order",
    "by","insert","update","delete","create","drop","alter","into",
    "and","or","not","null","true","false","primary","key","constraint"
}

def _prune_cache():
    if len(_CACHE) > _CACHE_MAX:
        # 무작위 삭제로 간단히 조절
        for _ in range(len(_CACHE) // 4):
            _CACHE.pop(next(iter(_CACHE)), None)

def cache_get(k: str) -> Optional[str]:
    return _CACHE.get(k)

def cache_set(k: str, v: str):
    _CACHE[k] = v
    _prune_cache()

def to_ascii_simple(text: str) -> str:
    """
    유니코드를 단순 ASCII로 축소.
    (의존성 없이 구현: 복잡한 음차는 제거되지만 DB 식별자엔 충분)
    """
    return (
        text.replace(" ", "_")
            .encode("ascii", "ignore")
            .decode("ascii")
    )

def to_snake_case(text: str) -> str:
    """
    공백/기호 → '_' 치환 후 snake_case 정규화
    """
    t = re.sub(r"[^\w\s-]", " ", text)   # 기호 제거
    t = re.sub(r"[-\s]+", "_", t)        # 공백/하이픈 → _
    t = re.sub(r"_+", "_", t)            # 연속 _ 정리
    return t.strip("_").lower()

def sanitize_table_name(name: str, max_len: int = 64) -> str:
    """
    - ASCII 축소
    - snake_case
    - 숫자로 시작 시 접두어 추가
    - 예약어 회피
    - 길이 제한
    """
    if not name:
        name = "untitled"

    name = to_ascii_simple(name)
    name = to_snake_case(name)
    if not name:
        name = "untitled"

    if name[0].isdigit():
        name = f"t_{name}"

    if name in _SQL_RESERVED:
        name = f"{name}_tbl"

    if len(name) > max_len:
        name = name[:max_len]

    return name or "untitled"

def ensure_unique(base: str) -> str:
    """
    충돌 방지를 위한 유니크 보정. (초/난수 꼬리표)
    """
    suffix = f"{int(time.time())%100000}_{random.randint(100,999)}"
    candidate = f"{base}_{suffix}"
    return candidate

def postprocess_translated_name(raw: str) -> str:
    """
    LLM 응답에서 코드블럭/따옴표/불필요 텍스트 제거 후 sanitize
    """
    if not raw:
        return "untitled"

    # 코드블럭/따옴표 제거
    raw = raw.strip()
    raw = raw.strip("`").strip('"').strip("'")

    # 흔한 설명 텍스트 제거 시도
    raw = re.sub(r"\b(translation|translated name|english name)\b[:\-]\s*", "", raw, flags=re.I)

    return sanitize_table_name(raw)
