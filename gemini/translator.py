import json
from typing import Optional
from .config import GEMINI_API_KEY, HTTP_TIMEOUT, session, pick_model, api_url
from .utils import *

_PROMPT_TMPL = (
    "You are naming a SQL table/column. "
    "Translate the following Korean name into a short English identifier. "
    "Rules: snake_case, ASCII only, no spaces, no punctuation, avoid reserved words, "
    "max 64 chars, and RETURN ONLY THE NAME.\n\n"
    "Korean: {kname}"
)

def _call_gemini(korean_name: str) -> Optional[str]:
    if not GEMINI_API_KEY:
        return None

    payload = {
        "contents": [{
            "parts": [{"text": _PROMPT_TMPL.format(kname=korean_name)}]
        }]
    }

    # 모델 자동 선택
    model = pick_model()

    try:
        r = session.post(
            api_url(model),
            params={"key": GEMINI_API_KEY},
            data=json.dumps(payload),
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            # 상세 에러 로깅
            try:
                err = r.json()
            except Exception:
                err = {"raw": r.text}
            print(f"[Gemini ERROR] model={model} status={r.status_code} body={err}")
            return None

        data = r.json()
        cand = (
            data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text")
        )
        return cand.strip() if isinstance(cand, str) else None
    except Exception as e:
        print(f"[Gemini EXC] model={model} exc={e}")
        return None

def translate_table_name(korean_name: str, ensure_unique_suffix: bool = False) -> str:
    key = f"tname::{korean_name}"
    cached = cache_get(key)
    if cached:
        return cached

    llm = _call_gemini(korean_name)
    if llm:
        name = postprocess_translated_name(llm)
    if ensure_unique_suffix:
        name = ensure_unique(name)

    cache_set(key, name)
    return name
