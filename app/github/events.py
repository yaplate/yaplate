import time
from typing import Any, Dict

from app.logger import get_logger
from app.github.comments import handle_comment
from app.commands.greet import greet_if_first_issue, greet_if_first_pr
from app.cache.store import (
    schedule_followup,
    cancel_followup,
    cancel_stale,
    purge_repo,
    purge_all,
    migrate_repo,
    mark_repo_installed,
    unmark_repo_installed,
    clear_followup_stopped,
    clear_followup_completed
)
from app.nlp.language_detect import detect_with_fallback
from app.settings import FOLLOWUP_DEFAULT_INTERVAL_HOURS
from app.github.api import RepoUnavailable


logger = get_logger("yaplate.github.events")


async def handle_event(event_type: str, payload: Dict[str, Any]):
    """
    Central GitHub webhook dispatcher.

    Installation-aware and resilient to:
    - missed webhooks
    - bot downtime
    - repo removals / renames
    """

    try:
        # ---------------------------------------------------------
        # 1. App uninstalled -> purge EVERYTHING
        # ---------------------------------------------------------
        if event_type == "installation" and payload.get("action") == "deleted":
            logger.info("App uninstalled — purging all state")
            purge_all()
            return

        # ---------------------------------------------------------
        # 2. App installed or repos added -> mark installed
        # ---------------------------------------------------------
        if event_type == "installation" and payload.get("action") == "created":
            for repo in payload.get("repositories", []):
                mark_repo_installed(repo["full_name"])
            return

        if event_type == "installation_repositories":
            action = payload.get("action")

            if action == "added":
                for repo in payload.get("repositories_added", []):
                    mark_repo_installed(repo["full_name"])
                return

            if action == "removed":
                for repo in payload.get("repositories_removed", []):
                    unmark_repo_installed(repo["full_name"])
                return

        # ---------------------------------------------------------
        # 3. Repository renamed -> migrate all state
        # ---------------------------------------------------------
        if event_type == "repository" and payload.get("action") == "renamed":
            repo = payload.get("repository") or {}
            old_full = repo.get("full_name")

            owner = repo.get("owner", {}).get("login")
            new_name = repo.get("name")

            if old_full and owner and new_name:
                new_full = f"{owner}/{new_name}"
                logger.info("Repository renamed: %s -> %s", old_full, new_full)
                migrate_repo(old_full, new_full)

            return

        # ---------------------------------------------------------
        # 4. All remaining events MUST have repository
        # ---------------------------------------------------------
        repository = payload.get("repository")
        if not repository:
            return

        repo_full = repository.get("full_name")
        repo_id = repository.get("id")

        if not repo_full or not repo_id:
            return

        # Defensive: ensure repo is marked installed if we see traffic
        mark_repo_installed(repo_full)

        # ---------------------------------------------------------
        # 5. Comment events
        # ---------------------------------------------------------
        if event_type in ("issue_comment", "pull_request_review_comment"):
            try:
                await handle_comment(payload)
            except RepoUnavailable:
                issue = payload.get("issue")
                if issue:
                    cancel_followup(repo_full, issue["number"])
                    cancel_stale(repo_full, issue["number"])
            return

        # ---------------------------------------------------------
        # 6. Issue events
        # ---------------------------------------------------------
        if event_type == "issues":
            action = payload.get("action")
            issue = payload.get("issue") or {}

            issue_number = issue.get("number")
            title = issue.get("title", "")
            body = issue.get("body") or ""

            if issue_number is None:
                return

            try:
                if action == "opened":
                    username = issue.get("user", {}).get("login")
                    if username:
                        await greet_if_first_issue(
                            repo_id,
                            repo_full,
                            issue_number,
                            username,
                            title,
                            body,
                        )

                elif action == "assigned":
                    assignee = payload.get("assignee", {}).get("login")
                    if assignee:
                        clear_followup_completed(repo_full, issue_number)
                        clear_followup_stopped(repo_full, issue_number)

                        lang = await detect_with_fallback(title, body)
                        due_at = time.time() + FOLLOWUP_DEFAULT_INTERVAL_HOURS * 3600

                        schedule_followup(
                            repo=repo_full,
                            issue_number=issue_number,
                            assignee=assignee,
                            lang=lang,
                            due_at=due_at,
                        )

                elif action in ("unassigned", "closed", "deleted"):
                    cancel_followup(repo_full, issue_number)
                    cancel_stale(repo_full, issue_number)
                    clear_followup_stopped(repo_full, issue_number)
                    clear_followup_completed(repo_full, issue_number)


            except RepoUnavailable:
                cancel_followup(repo_full, issue_number)
                cancel_stale(repo_full, issue_number)

            return

        # ---------------------------------------------------------
        # 7. Pull request events
        # ---------------------------------------------------------
        if event_type == "pull_request":
            action = payload.get("action")
            pr = payload.get("pull_request") or {}

            pr_number = pr.get("number")
            title = pr.get("title", "")
            body = pr.get("body") or ""
            author = pr.get("user", {}).get("login")

            if pr_number is None:
                return

            try:
                if action == "opened" and author:
                    await greet_if_first_pr(
                        repo_id,
                        repo_full,
                        pr_number,
                        author,
                        title,
                        body,
                    )

                    lang = await detect_with_fallback(title, body)
                    due_at = time.time() + FOLLOWUP_DEFAULT_INTERVAL_HOURS * 3600

                    schedule_followup(
                        repo=repo_full,
                        issue_number=pr_number,
                        assignee=author,
                        lang=lang,
                        due_at=due_at,
                    )

                elif action in ("closed", "converted_to_draft"):
                    cancel_followup(repo_full, pr_number)
                    cancel_stale(repo_full, pr_number)

            except RepoUnavailable:
                cancel_followup(repo_full, pr_number)
                cancel_stale(repo_full, pr_number)

            return

    except Exception:
        # Never crash webhook processing
        logger.exception("Unhandled error while processing event: %s", event_type)
