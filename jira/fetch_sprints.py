import dlt
from typing import Iterable, Optional
import requests
import logging

logger = logging.getLogger(__name__)

def fetch_boards(subdomain: str, email: str, api_token: str, board_type: Optional[str] = None) -> Iterable[dict]:
    """Fetch all boards (scrum, kanban, etc) from Jira."""
    url = f"https://{subdomain}.atlassian.net/rest/agile/1.0/board"
    auth = (email, api_token)
    headers = {"Accept": "application/json"}
    start_at = 0
    while True:
        params = {"startAt": start_at, "maxResults": 50}
        if board_type:
            params["type"] = board_type
        resp = requests.get(url, auth=auth, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        boards = data.get("values", [])
        for board in boards:
            yield board
        if start_at + len(boards) >= data.get("total", 0):
            break
        start_at += len(boards)

def fetch_sprints_for_board(subdomain: str, email: str, api_token: str, board_id: int) -> Iterable[dict]:
    """Fetch all sprints for a given board from Jira."""
    url = f"https://{subdomain}.atlassian.net/rest/agile/1.0/board/{board_id}/sprint"
    auth = (email, api_token)
    headers = {"Accept": "application/json"}
    start_at = 0
    while True:
        params = {"startAt": start_at, "maxResults": 50}
        try:
            resp = requests.get(url, auth=auth, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
            sprints = data.get("values", [])
            for sprint in sprints:
                sprint["board_id"] = board_id
                yield sprint
            if data.get("isLast", True):
                break
            start_at += len(sprints)
        except requests.HTTPError as ex:
            status = ex.response.status_code if ex.response is not None else None
            logger.warning("Skipping board %s due to error: %s %s", board_id, status, ex)
            break
        except Exception as ex:
            logger.warning("Unexpected error for board %s: %s", board_id, ex)
            break
