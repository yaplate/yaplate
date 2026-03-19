import time
from typing import Optional

import httpx
import jwt

from app.logger import get_logger
from app.settings import GITHUB_APP_ID, GITHUB_PRIVATE_KEY_PATH, GITHUB_PRIVATE_KEY


logger = get_logger("yaplate.github.auth")

_PRIVATE_KEY: Optional[str] = None
_CACHED_TOKEN: Optional[str] = None
_TOKEN_EXPIRY: float = 0.0


def _load_private_key() -> str:
    """
    Load GitHub App private key from:
    1) env var GITHUB_PRIVATE_KEY (recommended for Production)
    2) file path GITHUB_PRIVATE_KEY_PATH (recommended for local dev)
    """
    global _PRIVATE_KEY

    if _PRIVATE_KEY is not None:
        return _PRIVATE_KEY

    # 1) Railway / production: env var
    if GITHUB_PRIVATE_KEY and GITHUB_PRIVATE_KEY.strip():
        _PRIVATE_KEY = GITHUB_PRIVATE_KEY.strip()
        return _PRIVATE_KEY

    # 2) Local dev: file path
    if not GITHUB_PRIVATE_KEY_PATH:
        raise RuntimeError(
            "GitHub private key not configured. "
            "Set GITHUB_PRIVATE_KEY (recommended) or GITHUB_PRIVATE_KEY_PATH."
        )

    try:
        with open(GITHUB_PRIVATE_KEY_PATH, "r", encoding="utf-8") as f:
            _PRIVATE_KEY = f.read()
            return _PRIVATE_KEY
    except OSError as exc:
        raise RuntimeError(
            f"Failed to read GitHub private key at {GITHUB_PRIVATE_KEY_PATH}"
        ) from exc


def create_jwt() -> str:
    if not GITHUB_APP_ID:
        raise RuntimeError("GITHUB_APP_ID is not set")

    now = int(time.time())
    payload = {
        "iat": now - 30,
        "exp": now + 9 * 60,
        "iss": int(GITHUB_APP_ID),
    }

    private_key = _load_private_key()
    return jwt.encode(payload, private_key, algorithm="RS256")


async def get_installation_token() -> str:
    """
    Return a GitHub installation access token.
    Cached until expiry to avoid unnecessary regeneration.
    """
    global _CACHED_TOKEN, _TOKEN_EXPIRY

    now = time.time()
    if _CACHED_TOKEN and now < _TOKEN_EXPIRY:
        return _CACHED_TOKEN

    jwt_token = create_jwt()

    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github+json",
    }

    async with httpx.AsyncClient() as client:
        installations_resp = await client.get(
            "https://api.github.com/app/installations",
            headers=headers,
        )
        installations_resp.raise_for_status()

        installations = installations_resp.json()
        if not installations:
            raise RuntimeError("No GitHub App installations found")

        installation_id = installations[0]["id"]

        token_resp = await client.post(
            f"https://api.github.com/app/installations/{installation_id}/access_tokens",
            headers=headers,
        )
        token_resp.raise_for_status()

        data = token_resp.json()

        _CACHED_TOKEN = data["token"]
        _TOKEN_EXPIRY = time.time() + 50 * 60  # 1 hour minus buffer

        logger.info("GitHub installation token obtained")
        return _CACHED_TOKEN
