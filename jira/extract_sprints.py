import dlt
from typing import Iterable, Optional, List

def extract_issue_sprints(issue: dict) -> Iterable[dict]:
    """Yield all sprints (active and closed) for a given issue, with relevant metadata."""
    # Sprints may be in customfield_10020 (active/current), closedSprints, or customfield_10014 (legacy)
    sprints = []
    # Jira Cloud: customfield_10020 is often the 'Sprint' field (list of dicts or stringified JSON)
    cf_10020 = issue.get("fields", {}).get("customfield_10020")
    if cf_10020:
        if isinstance(cf_10020, list):
            sprints.extend(cf_10020)
        elif isinstance(cf_10020, str):
            import json
            try:
                sprints.extend(json.loads(cf_10020))
            except Exception:
                pass
    # closedSprints is sometimes present as a list
    closed = issue.get("fields", {}).get("closedSprints")
    if closed and isinstance(closed, list):
        sprints.extend(closed)
    # customfield_10014 is sometimes used for sprints (legacy)
    cf_10014 = issue.get("fields", {}).get("customfield_10014")
    if cf_10014:
        if isinstance(cf_10014, list):
            sprints.extend(cf_10014)
        elif isinstance(cf_10014, str):
            import json
            try:
                sprints.extend(json.loads(cf_10014))
            except Exception:
                pass
    # Deduplicate by id
    seen = set()
    for sprint in sprints:
        sprint_id = sprint.get("id") if isinstance(sprint, dict) else None
        if sprint_id and sprint_id not in seen:
            seen.add(sprint_id)
            yield sprint
