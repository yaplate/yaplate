import requests
import os
from github.getIssues import GETIssues
from github.installation_token import InstallationToken

get_issues = GETIssues()
installationToken = InstallationToken()


user = os.getenv("USER")
repo = os.getenv("REPO")

issue_number = get_issues.getIssueNumber()

assignees = get_issues.getIssueAssignees()

if assignees is not None:
    token = installationToken.getInstallationToken()

    url = f"https://api.github.com/repos/{user}/{repo}/issues/{issue_number}/comments"

    headers = {
            "Authorization" : f"Bearer {token}",
            "Accept" : "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    payload = {
        "body" : f"Hello {assignees}. Thank you for opening this issue, the reviewers will get back to you shortly!"
    }
    response = requests.post(url, headers=headers, json=payload)

else:
    print("Can't post a comment")