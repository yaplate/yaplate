import jwt
import time
import os
from dotenv import load_dotenv
load_dotenv()

app_id = os.getenv("GITHUB_APP_ID")
github_private_key = os.getenv("GITHUB_PRIVATE_KEY_PATH")

class GithubAuth:
    def __init__(self):
        self.auth_token = None

    def githubAuthentication(self):
        self.auth_token = None

        with open(github_private_key, "r") as file:
            private_key = file.read()

        payload = {
            "iat": int(time.time()) - 60,
            "exp": int(time.time()) + 600,
            "iss": app_id
        }

        self.auth_token = jwt.encode(payload, private_key, algorithm="RS256")