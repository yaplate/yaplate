import re
import asyncio
from typing import List
from collections import Counter

from app.logger import get_logger
from app.settings import LINGO_API_KEY
from app.nlp.gemini_client import detect_language_with_gemini
from lingodotdev.engine import LingoDotDevEngine

logger = get_logger("yaplate.nlp.language_detect")


# ---------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------

async def _detect_with_lingo(text: str) -> str | None:
    """
    Safe Lingo.dev detection.
    Returns ISO 639-1 code or None.
    """
    try:
        async with LingoDotDevEngine({"api_key": LINGO_API_KEY}) as engine:
            locale = await engine.recognize_locale(text)
            if isinstance(locale, str) and len(locale) == 2:
                return locale.lower()
    except Exception:
        logger.exception("Lingo locale detection failed")

    return None


# ---------------------------------------------------------
# Public API (behavior-compatible)
# ---------------------------------------------------------

async def detect_with_fallback(title: str, body: str) -> str:
    title = (title or "").strip()
    body = (body or "").strip()

    # 🔒 HARD RULE: If body is empty, force English (unchanged)
    if not body:
        return "en"

    # Split body into meaningful chunks (same as before)
    parts = re.split(r"[。\n.!?]", body)
    texts: List[str] = [p.strip() for p in parts if len(p.strip()) > 10]

    # If body chunks are too small -> force English (unchanged)
    if not texts:
        return "en"

    # --------------------------------------------------
    # 1️⃣ Primary detection: Lingo.dev (async, parallel)
    # --------------------------------------------------
    tasks = [_detect_with_lingo(t) for t in texts]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    langs = [r for r in results if isinstance(r, str)]
    if langs:
        freq = Counter(langs)
        dominant, count = freq.most_common(1)[0]

        # High confidence (same rule as before)
        if count > 1:
            return dominant

    # --------------------------------------------------
    # 2️⃣ Low confidence -> Gemini fallback (unchanged)
    # --------------------------------------------------
    combined = f"Title: {title}\n\nBody: {body}"
    try:
        gemini_lang = await detect_language_with_gemini(combined)
        if isinstance(gemini_lang, str):
            gemini_lang = gemini_lang.strip().lower()
            if len(gemini_lang) == 2:
                return gemini_lang
    except Exception:
        logger.exception("Gemini language detection failed")

    # --------------------------------------------------
    # 3️⃣ Final fallback (same behavior)
    # --------------------------------------------------
    if langs:
        return dominant

    return "en"
