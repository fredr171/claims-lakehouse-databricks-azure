"""GitHub's REST API as an IssueTracker, with the standard library only.

The token is a fine-grained personal access token limited to this repository with
read/write access to issues; the notifier Function reads it from Key Vault.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable
from typing import Any

from claims_lakehouse.notifier.issues import Issue

API = "https://api.github.com"
Opener = Callable[[urllib.request.Request], Any]


class GitHubIssues:
    def __init__(self, repository: str, token: str, opener: Opener | None = None) -> None:
        self.repository = repository  # owner/name
        self._token = token
        self._open = opener or (lambda request: urllib.request.urlopen(request, timeout=30))

    def _call(self, method: str, path: str, payload: dict | None = None) -> Any:
        request = urllib.request.Request(
            f"{API}/repos/{self.repository}{path}",
            data=None if payload is None else json.dumps(payload).encode(),
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "claims-lakehouse-notifier",
            },
        )
        with self._open(request) as response:
            body = response.read()
        return json.loads(body) if body else None

    def open_issues(self, label: str) -> list[Issue]:
        issues: list[Issue] = []
        page = 1
        while True:
            batch = self._call("GET", f"/issues?state=open&labels={label}&per_page=100&page={page}")
            # The issues API also lists pull requests.
            issues += [
                Issue(i["number"], i["title"], i.get("body") or "")
                for i in batch
                if "pull_request" not in i
            ]
            if len(batch) < 100:
                return issues
            page += 1

    def create(self, title: str, body: str, labels: list[str]) -> int:
        return self._call("POST", "/issues", {"title": title, "body": body, "labels": labels})[
            "number"
        ]

    def comment(self, number: int, body: str) -> None:
        self._call("POST", f"/issues/{number}/comments", {"body": body})

    def close(self, number: int, comment: str) -> None:
        self.comment(number, comment)
        self._call("PATCH", f"/issues/{number}", {"state": "closed", "state_reason": "completed"})
