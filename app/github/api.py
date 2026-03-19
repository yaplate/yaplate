import httpx
from typing import Any, Optional

from app.github.auth import get_installation_token
from app.logger import get_logger


GITHUB_API = "https://api.github.com"

logger = get_logger("yaplate.github.api")


class RepoUnavailable(Exception):
    """
    Raised when a repository is deleted, renamed, or the app lost access.
    """
    pass


async def _headers() -> dict[str, str]:
    token = await get_installation_token()
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }


async def _request(
    method: str,
    endpoint: str,
    json: Optional[dict] = None,
) -> Any:
    headers = await _headers()
    url = f"{GITHUB_API}{endpoint}"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.request(
            method,
            url,
            headers=headers,
            json=json,
        )

    status = response.status_code

    if status in (404, 410):
        logger.warning("Repo unavailable (%s): %s", status, endpoint)
        raise RepoUnavailable(f"Repository or resource not found: {endpoint}")

    if status in (401, 403):
        logger.warning("Access denied (%s): %s", status, endpoint)
        raise RepoUnavailable(f"Access denied or app uninstalled: {endpoint}")

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.exception("GitHub API error %s for %s", status, endpoint)
        raise

    if status == 204:
        return None

    try:
        return response.json()
    except ValueError:
        logger.exception("Failed to decode JSON response from %s", endpoint)
        raise


# Public helpers
async def github_post(endpoint: str, json: dict):
    return await _request("POST", endpoint, json)


async def github_patch(endpoint: str, json: dict):
    return await _request("PATCH", endpoint, json)


async def github_get(endpoint: str):
    return await _request("GET", endpoint)


async def github_delete(endpoint: str) -> bool:
    await _request("DELETE", endpoint)
    return True


async def get_issue_comments(repo: str, issue_number: int):
    return await github_get(
        f"/repos/{repo}/issues/{issue_number}/comments?per_page=100"
    )


async def get_user_issues(repo: str, username: str):
    return await github_get(
        f"/search/issues?q=repo:{repo}+type:issue+author:{username}"
    )


async def get_user_prs(repo: str, username: str):
    return await github_get(
        f"/search/issues?q=repo:{repo}+type:pr+author:{username}"
    )


async def get_repo_maintainers(repo: str):
    owners = await github_get(
        f"/repos/{repo}/collaborators?permission=maintain"
    )
    admins = await github_get(
        f"/repos/{repo}/collaborators?permission=admin"
    )

    users = set()
    for u in owners + admins:
        users.add(u["login"])

    return list(users)


async def list_installed_repos():
    return await github_get("/installation/repositories")


async def list_open_assigned_issues(repo: str):
    return await github_get(
        f"/repos/{repo}/issues?state=open&assignee=*"
    )
