import asyncio
import time
from typing import Any, Dict

from app.logger import get_logger
from app.github.api import (
    github_post,
    github_patch,
    github_delete,
    get_repo_maintainers,
)
from app.commands.summarize import summarize_thread
from app.commands.parser import (
    parse_summarize_command,
    parse_translate_command,
    parse_reply_command,
)
from app.commands.translate import translate_and_format
from app.commands.reply import build_proxy_reply
from app.cache.store import (
    set_comment_mapping,
    get_comment_mapping,
    delete_comment_mapping,
    cancel_followup,
    cancel_stale,
    reschedule_followup,
    get_followup_data,
    mark_followup_stopped,
    mark_followup_completed,
    has_followup
)
from app.settings import FOLLOWUP_DEFAULT_INTERVAL_HOURS, MAX_FOLLOWUP_ATTEMPTS, STOPPING_ESCALATION_MAINTAINERS, STOPPING_ESCALATION_HARD_STOP
from app.nlp.context_builder import build_reply_context
from app.nlp.semantic_check import wants_maintainer_attention

logger = get_logger("yaplate.github.comments")

BOT_NAME = "yaplate"


# Helpers
def is_pure_quote(comment_body: str) -> bool:
    lines = [l.strip() for l in comment_body.strip().splitlines()]
    return bool(lines) and all(line.startswith(">") for line in lines)


def extract_user_text(comment_body: str) -> str:
    return "\n".join(
        line for line in comment_body.splitlines()
        if not line.strip().startswith(">")
    ).strip()


async def stop_followups_with_notice(
    repo: str,
    issue_number: int,
    reason: str,
    mention_maintainers: bool = True,
):
    """
    Hard-stop follow-up & stale automation and post a visible explanation.
    This is the ONLY place where mark_followup_stopped() is allowed.
    """
    mentions = ""

    if mention_maintainers:
        maintainers = await get_repo_maintainers(repo)
        if maintainers:
            mentions = " ".join(f"@{m}" for m in maintainers)

    body = (
        f"{mentions}\n\n"
        f"{reason}"
    ).strip()

    await github_post(
        f"/repos/{repo}/issues/{issue_number}/comments",
        {"body": body},
    )

    cancel_followup(repo, issue_number)
    cancel_stale(repo, issue_number)
    mark_followup_stopped(repo, issue_number)



# Main handler
async def handle_comment(payload: Dict[str, Any]):
    try:
        action = payload.get("action")
        comment = payload.get("comment") or {}

        comment_id = comment.get("id")
        comment_body = comment.get("body", "")
        comment_user = comment.get("user", {}).get("login")

        if not comment_id or not comment_user:
            return

        # Ignore bot's own comments
        user_lower = comment_user.lower()
        if user_lower.endswith("[bot]") or user_lower == BOT_NAME:
            return

        repository = payload.get("repository") or {}
        issue = payload.get("issue") or {}

        repo = repository.get("full_name")
        issue_number = issue.get("number")

        if not repo or issue_number is None:
            return
        
        followup_exists = has_followup(repo, issue_number)

        is_bot_command = f"@{BOT_NAME}" in comment_body.lower()

        # 1. Pure quote -> hard stop (explicit disengagement)
        if action == "created" and followup_exists and is_pure_quote(comment_body):
            await stop_followups_with_notice(
                repo,
                issue_number,
                reason= STOPPING_ESCALATION_HARD_STOP,
                mention_maintainers=False,
            )
            return

        # 2. Quote reply + human text -> possible escalation
        if (
            action == "created"
            and comment_body.lstrip().startswith(">")
            and not is_bot_command
        ):
            user_text = extract_user_text(comment_body)

            if followup_exists and user_text and await wants_maintainer_attention(user_text):
                await stop_followups_with_notice(
                    repo,
                    issue_number,
                    reason=STOPPING_ESCALATION_MAINTAINERS,
                    mention_maintainers=True,
                )
                return

            # Otherwise: quote reply = acknowledgement -> pause only
            cancel_stale(repo, issue_number)

            key = f"yaplate:followup:{repo}:{issue_number}"
            data = get_followup_data(key)
            if data:
                attempt = int(data.get("attempt", 0))
                if attempt < MAX_FOLLOWUP_ATTEMPTS:
                    next_due = time.time() + FOLLOWUP_DEFAULT_INTERVAL_HOURS * 3600
                    reschedule_followup(repo, issue_number, next_due)
                else:
                    cancel_followup(repo, issue_number)
                    mark_followup_completed(repo, issue_number)
            return

        # 3. Normal human reply -> progress or pause
        if action == "created" and not is_bot_command:            
            cancel_stale(repo, issue_number)

            # Plain-text maintainer wait -> stop WITH notice
            if followup_exists and await wants_maintainer_attention(comment_body):
                await stop_followups_with_notice(
                    repo,
                    issue_number,
                    reason= STOPPING_ESCALATION_MAINTAINERS,
                    mention_maintainers=True,
                )
                return

            # Otherwise: normal progress -> reschedule follow-up
            key = f"yaplate:followup:{repo}:{issue_number}"
            data = get_followup_data(key)

            if data:
                attempt = int(data.get("attempt", 0))
                if attempt < MAX_FOLLOWUP_ATTEMPTS:
                    next_due = time.time() + FOLLOWUP_DEFAULT_INTERVAL_HOURS * 3600
                    reschedule_followup(repo, issue_number, next_due)
                else:
                    cancel_followup(repo, issue_number)
                    mark_followup_completed(repo, issue_number)

        # 4. User deleted comment -> remove bot mirror
        if action == "deleted":
            await asyncio.sleep(1.5)

            bot_comment_id = get_comment_mapping(comment_id)
            if bot_comment_id:
                try:
                    await github_delete(
                        f"/repos/{repo}/issues/comments/{bot_comment_id}"
                    )
                except Exception:
                    logger.exception(
                        "Failed to delete mirrored bot comment: %s",
                        bot_comment_id,
                    )

                delete_comment_mapping(comment_id)
            return

        # 5. Parse bot commands
        summarize_parsed = parse_summarize_command(comment_body)
        reply_parsed = parse_reply_command(comment_body)
        translate_parsed = parse_translate_command(comment_body)

        if action == "edited" and not (
            summarize_parsed or reply_parsed or translate_parsed
        ):
            return

        if summarize_parsed:
            final_reply = await summarize_thread(
                repo=repo,
                issue_number=issue_number,
                target_lang=summarize_parsed["target_lang"],
                trigger_text=comment_body,
            )

        elif reply_parsed:
            ctx = build_reply_context(payload)
            final_reply = await build_proxy_reply(
                parent_text=reply_parsed["parent_text"],
                speaker_text=reply_parsed["speaker_text"],
                speaker_username=ctx["speaker_username"],
                target_lang=reply_parsed["target_lang"],
            )

        elif translate_parsed:
            final_reply = await translate_and_format(
                translate_parsed["quoted_text"],
                target_lang=translate_parsed["target_lang"],
                quoted_label=translate_parsed.get("quoted_label"),
                user_message=comment_body,
            )
        else:
            return

        # 6. Redis-backed reply mapping
        await asyncio.sleep(1.5)

        if action == "created":
            response = await github_post(
                f"/repos/{repo}/issues/{issue_number}/comments",
                {"body": final_reply},
            )
            set_comment_mapping(comment_id, response["id"])

        elif action == "edited":
            bot_comment_id = get_comment_mapping(comment_id)

            if bot_comment_id:
                await github_patch(
                    f"/repos/{repo}/issues/comments/{bot_comment_id}",
                    {"body": final_reply},
                )
            else:
                response = await github_post(
                    f"/repos/{repo}/issues/{issue_number}/comments",
                    {"body": final_reply},
                )
                set_comment_mapping(comment_id, response["id"])

    except Exception:
        # Never crash comment processing
        logger.exception("Unhandled error while processing comment event")
