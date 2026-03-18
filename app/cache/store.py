import time
from typing import Iterable

from app.cache.keys import (
    KEY_PREFIX,
    FIRST_ISSUE_PREFIX,
    FIRST_PR_PREFIX,
    FOLLOWUP_PREFIX,
    FOLLOWUP_INDEX,
    STALE_PREFIX,
    STALE_INDEX,
    INSTALLED_REPO_PREFIX,
    FOLLOWUP_STOPPED_PREFIX,
    FOLLOWUP_COMPLETED_PREFIX
)
from app.cache.redis_client import get_redis
from app.logger import get_logger


logger = get_logger("yaplate.cache.store")


# Utility
def _as_str(x):
    return x.decode() if isinstance(x, bytes) else x

def _safe_iter(keys: Iterable):
    for key in keys:
        yield _as_str(key)


# Repository installation state
def mark_repo_installed(repo: str):
    r = get_redis()
    try:
        r.set(f"{INSTALLED_REPO_PREFIX}{repo}", 1)
    except Exception:
        logger.exception("Failed to mark repo installed: %s", repo)


def unmark_repo_installed(repo: str):
    r = get_redis()
    try:
        r.delete(f"{INSTALLED_REPO_PREFIX}{repo}")
        purge_repo(repo)
    except Exception:
        logger.exception("Failed to unmark repo installed: %s", repo)


def is_repo_installed(repo: str) -> bool:
    r = get_redis()
    try:
        return bool(r.exists(f"{INSTALLED_REPO_PREFIX}{repo}"))
    except Exception:
        logger.exception("Failed to check repo installed state: %s", repo)
        return False


def get_all_installed_repos() -> set[str]:
    r = get_redis()
    repos = set()

    try:
        for key in r.scan_iter(f"{INSTALLED_REPO_PREFIX}*"):
            k = _as_str(key)
            repos.add(k.replace(INSTALLED_REPO_PREFIX, "", 1))
    except Exception:
        logger.exception("Failed to list installed repos")

    return repos


def purge_orphaned_repos(valid_repos: set[str]):
    r = get_redis()

    try:
        for key in _safe_iter(r.zrange(FOLLOWUP_INDEX, 0, -1)):
            repo = key.replace(FOLLOWUP_PREFIX, "").split(":", 1)[0]
            if repo not in valid_repos:
                purge_repo(repo)

        for key in _safe_iter(r.zrange(STALE_INDEX, 0, -1)):
            repo = key.replace(STALE_PREFIX, "").split(":", 1)[0]
            if repo not in valid_repos:
                purge_repo(repo)

    except Exception:
        logger.exception("Failed to purge orphaned repos")



# Comment <--> bot reply mapping
def set_comment_mapping(user_comment_id: int, bot_comment_id: int):
    r = get_redis()
    try:
        r.set(f"{KEY_PREFIX}{user_comment_id}", bot_comment_id)
    except Exception:
        logger.exception("Failed to set comment mapping: %s", user_comment_id)


def get_comment_mapping(user_comment_id: int):
    r = get_redis()
    try:
        return r.get(f"{KEY_PREFIX}{user_comment_id}")
    except Exception:
        logger.exception("Failed to get comment mapping: %s", user_comment_id)
        return None


def delete_comment_mapping(user_comment_id: int):
    r = get_redis()
    try:
        r.delete(f"{KEY_PREFIX}{user_comment_id}")
    except Exception:
        logger.exception("Failed to delete comment mapping: %s", user_comment_id)


# Greeting tracking
def has_been_greeted(repo_id: int, username: str) -> bool:
    r = get_redis()
    try:
        return r.exists(f"{FIRST_ISSUE_PREFIX}{repo_id}:{username}")
    except Exception:
        logger.exception("Failed to check greeting state")
        return False


def mark_greeted(repo_id: int, username: str):
    r = get_redis()
    try:
        r.set(f"{FIRST_ISSUE_PREFIX}{repo_id}:{username}", 1)
    except Exception:
        logger.exception("Failed to mark greeted")


# Greeting seeding (startup reconciliation)
def mark_user_seen(repo_id: int, username: str):
    """
    Mark a user as already seen in this repo.
    Used during startup reconciliation to avoid false
    'first issue' greetings after downtime.
    """
    if not username:
        return

    r = get_redis()
    try:
        r.set(f"{FIRST_ISSUE_PREFIX}{repo_id}:{username}", 1)
    except Exception:
        logger.exception(
            "Failed to mark user seen: repo_id=%s user=%s",
            repo_id,
            username,
        )

def has_been_greeted_pr(repo_id: int, username: str) -> bool:
    r = get_redis()
    try:
        return r.exists(f"{FIRST_PR_PREFIX}{repo_id}:{username}")
    except Exception:
        logger.exception("Failed to check PR greeting state")
        return False

def mark_greeted_pr(repo_id: int, username: str):
    r = get_redis()
    try:
        r.set(f"{FIRST_PR_PREFIX}{repo_id}:{username}", 1)
    except Exception:
        logger.exception("Failed to mark PR greeted")



# Follow-up scheduling
def schedule_followup(repo: str, issue_number: int, assignee: str, lang: str, due_at: float, attempt: int = 1):
    if not is_repo_installed(repo):
        return

    r = get_redis()
    key = f"{FOLLOWUP_PREFIX}{repo}:{issue_number}"

    try:
        r.hset(key, mapping={
            "repo": repo,
            "issue_number": issue_number,
            "assignee": assignee,
            "lang": lang,
            "due_at": due_at,
            "sent": 0,
            "attempt": attempt,
        })
        r.zadd(FOLLOWUP_INDEX, {key: due_at})
    except Exception:
        logger.exception("Failed to schedule followup: %s #%s", repo, issue_number)


def reschedule_followup(repo: str, issue_number: int, next_due_at: float):
    r = get_redis()
    key = f"{FOLLOWUP_PREFIX}{repo}:{issue_number}"

    try:
        data = r.hgetall(key)
        if not data or not is_repo_installed(repo):
            cancel_followup(repo, issue_number)
            return

        attempt = int(data.get("attempt", 1)) + 1

        r.hset(key, mapping={
            "due_at": next_due_at,
            "sent": 0,
            "attempt": attempt,
        })
        r.zadd(FOLLOWUP_INDEX, {key: next_due_at})
    except Exception:
        logger.exception("Failed to reschedule followup: %s #%s", repo, issue_number)


def cancel_followup(repo: str, issue_number: int):
    r = get_redis()
    key = f"{FOLLOWUP_PREFIX}{repo}:{issue_number}"

    try:
        r.delete(key)
        r.zrem(FOLLOWUP_INDEX, key)

        stale_key = f"{STALE_PREFIX}{repo}:{issue_number}"
        r.delete(stale_key)
        r.zrem(STALE_INDEX, stale_key)
    except Exception:
        logger.exception("Failed to cancel followup: %s #%s", repo, issue_number)


def get_due_followups(now: float):
    r = get_redis()
    try:
        return r.zrangebyscore(FOLLOWUP_INDEX, 0, now)
    except Exception:
        logger.exception("Failed to get due followups")
        return []


def mark_followup_sent(key: str):
    r = get_redis()
    try:
        r.hset(key, "sent", 1)
        r.zrem(FOLLOWUP_INDEX, key)
    except Exception:
        logger.exception("Failed to mark followup sent: %s", key)


def get_followup_data(key: str):
    r = get_redis()
    try:
        return r.hgetall(key)
    except Exception:
        logger.exception("Failed to get followup data: %s", key)
        return {}


def has_followup(repo: str, issue_number: int) -> bool:
    r = get_redis()
    try:
        return r.exists(f"{FOLLOWUP_PREFIX}{repo}:{issue_number}")
    except Exception:
        logger.exception("Failed to check followup existence")
        return False

def mark_followup_completed(repo: str, issue_number: int):
    r = get_redis()
    r.set(f"{FOLLOWUP_COMPLETED_PREFIX}{repo}:{issue_number}", 1)


def is_followup_completed(repo: str, issue_number: int) -> bool:
    r = get_redis()
    return bool(r.exists(f"{FOLLOWUP_COMPLETED_PREFIX}{repo}:{issue_number}"))


def clear_followup_completed(repo: str, issue_number: int):
    r = get_redis()
    r.delete(f"{FOLLOWUP_COMPLETED_PREFIX}{repo}:{issue_number}")


# Stale handling
def schedule_stale(repo: str, issue_number: int, lang: str, due_at: float):
    if not is_repo_installed(repo):
        return

    r = get_redis()
    key = f"{STALE_PREFIX}{repo}:{issue_number}"

    try:
        r.hset(key, mapping={
            "repo": repo,
            "issue_number": issue_number,
            "lang": lang,
            "due_at": due_at,
        })
        r.zadd(STALE_INDEX, {key: due_at})
    except Exception:
        logger.exception("Failed to schedule stale: %s #%s", repo, issue_number)


def cancel_stale(repo: str, issue_number: int):
    r = get_redis()
    key = f"{STALE_PREFIX}{repo}:{issue_number}"

    try:
        r.delete(key)
        r.zrem(STALE_INDEX, key)
    except Exception:
        logger.exception("Failed to cancel stale: %s #%s", repo, issue_number)


def get_due_stales(now: float):
    r = get_redis()
    try:
        return r.zrangebyscore(STALE_INDEX, 0, now)
    except Exception:
        logger.exception("Failed to get due stales")
        return []


def get_stale_data(key: str):
    r = get_redis()
    try:
        return r.hgetall(key)
    except Exception:
        logger.exception("Failed to get stale data: %s", key)
        return {}



# Repo-wide cleanup / migration
def purge_repo(repo: str):
    r = get_redis()

    try:
        for key in _safe_iter(r.zrange(FOLLOWUP_INDEX, 0, -1)):
            if key.startswith(f"{FOLLOWUP_PREFIX}{repo}:"):
                r.delete(key)
                r.zrem(FOLLOWUP_INDEX, key)

        for key in _safe_iter(r.zrange(STALE_INDEX, 0, -1)):
            if key.startswith(f"{STALE_PREFIX}{repo}:"):
                r.delete(key)
                r.zrem(STALE_INDEX, key)
    except Exception:
        logger.exception("Failed to purge repo: %s", repo)


def migrate_repo(old_repo: str, new_repo: str):
    r = get_redis()

    try:
        for key in _safe_iter(r.zrange(FOLLOWUP_INDEX, 0, -1)):
            if key.startswith(f"{FOLLOWUP_PREFIX}{old_repo}:"):
                new_key = key.replace(
                    f"{FOLLOWUP_PREFIX}{old_repo}:",
                    f"{FOLLOWUP_PREFIX}{new_repo}:",
                )
                score = r.zscore(FOLLOWUP_INDEX, key)
                r.rename(key, new_key)
                r.zrem(FOLLOWUP_INDEX, key)
                r.zadd(FOLLOWUP_INDEX, {new_key: score})

        for key in _safe_iter(r.zrange(STALE_INDEX, 0, -1)):
            if key.startswith(f"{STALE_PREFIX}{old_repo}:"):
                new_key = key.replace(
                    f"{STALE_PREFIX}{old_repo}:",
                    f"{STALE_PREFIX}{new_repo}:",
                )
                score = r.zscore(STALE_INDEX, key)
                r.rename(key, new_key)
                r.zrem(STALE_INDEX, key)
                r.zadd(STALE_INDEX, {new_key: score})

        r.delete(f"{INSTALLED_REPO_PREFIX}{old_repo}")
        r.set(f"{INSTALLED_REPO_PREFIX}{new_repo}", 1)

    except Exception:
        logger.exception("Failed to migrate repo: %s → %s", old_repo, new_repo)


def purge_all():
    r = get_redis()
    try:
        for key in r.scan_iter("yaplate:*"):
            r.delete(key)
    except Exception:
        logger.exception("Failed to purge all keys")


def mark_followup_stopped(repo: str, issue_number: int):
    r = get_redis()
    key = f"{FOLLOWUP_STOPPED_PREFIX}{repo}:{issue_number}"
    r.set(key, 1)

def is_followup_stopped(repo: str, issue_number: int) -> bool:
    r = get_redis()
    return bool(r.exists(f"{FOLLOWUP_STOPPED_PREFIX}{repo}:{issue_number}"))

def clear_followup_stopped(repo: str, issue_number: int):
    r = get_redis()
    r.delete(f"{FOLLOWUP_STOPPED_PREFIX}{repo}:{issue_number}")
