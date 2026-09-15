import os
import json
import requests
from github.installation_token import InstallationToken

user = os.getenv("USER")
repo = os.getenv("REPO")
installationToken = InstallationToken()

class GETIssues:
    def __init__(self):
        self.issueNumber = None
        self.assignees = None

    def getIssueNumber(self):
        self.issueNumber = None

        token = installationToken.getInstallationToken()
        url = f"https://api.github.com/repos/{user}/{repo}/issues"

        headers = {
            "Authorization" : f"Bearer {token}",
            "Accept" : "application/vnd.github+json",
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        issue = response.json()

        # print(json.dumps(issue, indent=4))
        print(issue[0]["number"])

        self.issueNumber = issue[0]["number"]

        return self.issueNumber

    def getIssueAssignees(self):
        self.assignees = None

        token = installationToken.getInstallationToken()
        url = f"https://api.github.com/repos/{user}/{repo}/issues"

        headers = {
            "Authorization" : f"Bearer {token}",
            "Accept" : "application/vnd.github+json",
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        issue = response.json()
        # print("Assignee:", issue[0]["assignees"])
        self.assignees = issue[0]["assignees"]

        return self.assignees

    def getIssueAuthor(self):
        pass
    
