import os
import json
import requests
from github.auth import GithubAuth

github_auth = GithubAuth()
github_auth.githubAuthentication()

user = os.getenv("USER")
repo = os.getenv("REPO")

# print(github_auth.auth_token)
# print(github_auth)

class InstallationToken:
    def __init__(self):
        self.url = None
        self.installtion_token = None

    def getInstallationToken(self):

        url_installation_id = f"https://api.github.com/repos/{user}/{repo}/installation"
        headers = {
            "Authorization" : f"Bearer {github_auth.auth_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }


        response = requests.get(url_installation_id, headers=headers)

        data_id = response.json()

        installation_id = data_id["id"]

        url_installation_token = f"https://api.github.com/app/installations/{installation_id}/access_tokens"

        response = requests.post(url_installation_token, headers=headers)
        response.raise_for_status()

        data_token = response.json()

        self.installtion_token = data_token["token"]

        return self.installtion_token