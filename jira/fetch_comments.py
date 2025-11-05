import dlt
from typing import Iterable, Optional, List
import requests

def fetch_issue_comments(subdomain: str, email: str, api_token: str, issue_id_or_key: str, page_size: int = 100) -> Iterable[dict]:
    """Fetch all comments for a given issue via Jira API."""
    url = f"https://{subdomain}.atlassian.net/rest/api/3/issue/{issue_id_or_key}/comment"
    auth = (email, api_token)
    headers = {"Accept": "application/json"}
    start_at = 0
    while True:
        params = {"startAt": start_at, "maxResults": page_size}
        resp = requests.get(url, auth=auth, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        comments = data.get("comments", [])
        for comment in comments:
            yield comment
        if start_at + len(comments) >= data.get("total", 0):
            break
        start_at += len(comments)
