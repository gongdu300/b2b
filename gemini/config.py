import os, requests
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(ENV_PATH)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL_VERSION = os.getenv("GEMINI_MODEL_VERSION", "gemini-1.5-pro")
PREFERRED      = os.getenv("GEMINI_MODEL", "").strip()  # 있으면 우선 사용
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "12"))

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})
# back/gemini/config.py
print(f"[Gemini CONFIG] key_loaded={bool(GEMINI_API_KEY)} key_len={len(GEMINI_API_KEY) if GEMINI_API_KEY else 0}")
print(f"[Gemini CONFIG] CWD={os.getcwd()}")
print(f"[Gemini CONFIG] ENV_PATH={ENV_PATH} exists={os.path.exists(ENV_PATH)}")
print(f"[Gemini CONFIG] key_loaded={bool(GEMINI_API_KEY)} key_len={len(GEMINI_API_KEY) if GEMINI_API_KEY else 0}")

def list_models():
    """
    내 API 키로 접근 가능한 모델 리스트를 반환.
    """
    url = "https://generativelanguage.googleapis.com/v1beta/models"
    r = session.get(url, params={"key": GEMINI_API_KEY}, timeout=8)
    r.raise_for_status()
    return r.json().get("models", [])

def pick_model():
    """
    generateContent 지원하는 flash 계열을 우선순위로 자동 선택.
    .env에 GEMINI_MODEL이 있으면 그걸 그대로 사용(실패 시 자동 대체).
    """
    if PREFERRED:
        return PREFERRED
    try:
        models = list_models()
        # generateContent 지원하는 모델만 필터
        def supports_generate(m):
            return "supportedGenerationMethods" in m and "generateContent" in m["supportedGenerationMethods"]

        names = [(m["name"].split("/")[-1], m) for m in models if supports_generate(m)]

        # 1순위: 2.5 flash
        for name, _ in names:
            if "2.5" in name and "flash" in name and "preview" not in name:
                return name
        # 2순위: flash
        for name, _ in names:
            if "flash" in name and "preview" not in name:
                return name
        # 3순위: 아무거나 generateContent 지원하는 첫 모델
        return names[0][0] if names else "gemini-2.5-flash"
    except Exception:
        return "gemini-2.5-flash"  # 안전 기본값

def api_url(model: str):
    return f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
