"""Jira source adapted for the new search/jql endpoint."""

from typing import Iterable, List, Optional

import dlt
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests

from .settings import DEFAULT_ENDPOINTS, DEFAULT_PAGE_SIZE
import os
import re
import datetime
import duckdb
import logging

# module logger. Debug output is gated behind the JIRA_DEBUG env var so
# we don't spam stdout during normal runs. Users can enable DEBUG by
# setting JIRA_DEBUG=1 in their environment or configure logging
# externally in their application.
logger = logging.getLogger(__name__)
if os.environ.get("JIRA_DEBUG"):
    logger.setLevel(logging.DEBUG)
else:
    # Set to INFO level to see pagination logs
    logger.setLevel(logging.INFO)


def clean_dict(d):
    """Remove None values from dictionary recursively."""
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items() if v is not None}
    return d


@dlt.source(max_table_nesting=1)
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Iterable[DltResource]:
    import os, re, datetime, duckdb

    def _get_last_updated_from_duckdb(db_path: str = "jira_pipeline.duckdb") -> Optional[str]:
        if not os.path.exists(db_path):
            return None
        try:
            conn = duckdb.connect(database=db_path, read_only=True)
            candidates = [
                "SELECT MAX(updated) FROM issues",
                "SELECT MAX(updated) FROM jira.issues",
                "SELECT MAX(updated) FROM \"jira\".issues",
            ]
            last = None
            for q in candidates:
                try:
                    cur = conn.execute(q)
                    val = cur.fetchone()
                    if val and val[0] is not None:
                        last = val[0]
                        break
                except Exception:
                    continue
            conn.close()
            if last is None:
                return None
            if isinstance(last, (datetime.datetime, datetime.date)):
                dt = last if isinstance(last, datetime.datetime) else datetime.datetime.combine(last, datetime.time())
                return dt.strftime("%Y/%m/%d %H:%M")
            s = str(last)
            m = re.match(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})", s)
            if m:
                return f"{m.group(1)}/{m.group(2)}/{m.group(3)} {m.group(4)}:{m.group(5)}"
            m = re.match(r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})", s)
            if m:
                return f"{m.group(1)}/{m.group(2)}/{m.group(3)} {m.group(4)}:{m.group(5)}"
            return None
        except Exception:
            return None

    resources = []
    last_updated = _get_last_updated_from_duckdb()

    # issues resource
    for endpoint_name, endpoint_parameters in DEFAULT_ENDPOINTS.items():
        ep_params = dict(endpoint_parameters.get("params", {}))
        if endpoint_name == "issues" and last_updated:
            base_project = ep_params.get("jql", "project = DT")
            base = re.split(r"ORDER BY", base_project, flags=re.IGNORECASE)[0].strip()
            incremental_jql = f"{base} AND updated >= \"{last_updated}\" ORDER BY updated DESC"
            ep_params["jql"] = incremental_jql
        new_endpoint_parameters = dict(endpoint_parameters)
        new_endpoint_parameters["params"] = ep_params
        # Use "replace" write disposition for issues to avoid duplicates
        write_disp = "replace" if endpoint_name == "issues" else None
        res_kwargs = {
            **new_endpoint_parameters,
            "subdomain": subdomain,
            "email": email,
            "api_token": api_token,
            "page_size": page_size,
        }
        if write_disp:
            res_function = dlt.resource(get_paginated_data, name=endpoint_name, write_disposition=write_disp)(**res_kwargs)
        else:
            res_function = dlt.resource(get_paginated_data, name=endpoint_name)(**res_kwargs)
        resources.append(res_function)

    # issue_histories resource
    @dlt.resource(write_disposition="append")
    def issue_histories(
        subdomain: str = subdomain,
        email: str = email,
        api_token: str = api_token,
        page_size: int = page_size,
        jql_queries: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
    ):
        if jql_queries is None:
            jql_queries = DEFAULT_ENDPOINTS.get("issues", {}).get("params", {}).get("jql", [])
        if fields is None:
            fields = DEFAULT_ENDPOINTS.get("issues", {}).get("params", {}).get("fields", [])
        logger.debug("issue_histories resource called")
        yielded = 0
        for jql in jql_queries if isinstance(jql_queries, list) else [jql_queries]:
            params = {"fields": fields, "jql": jql, "maxResults": page_size, "expand": "changelog,comment"}
            for issues_page in get_paginated_data(
                api_path="rest/api/3/search/jql",
                data_path=None,
                subdomain=subdomain,
                email=email,
                api_token=api_token,
                page_size=page_size,
                params=params,
            ):
                for idx, issue in enumerate(issues_page):
                    if idx == 0:
                        import pprint
                        logger.debug("RAW ISSUE (issue_histories):\n%s", pprint.pformat(issue))
                    histories = issue.get("changelog", {}).get("histories", [])
                    for history in histories:
                        author = history.get("author", {})
                        created = history.get("created")
                        if last_updated and created:
                            m = re.match(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})", str(created))
                            if m:
                                created_dt = datetime.datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5)))
                                lm = re.match(r"(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2})", str(last_updated))
                                if lm:
                                    last_dt = datetime.datetime(int(lm.group(1)), int(lm.group(2)), int(lm.group(3)), int(lm.group(4)), int(lm.group(5)))
                                    if created_dt <= last_dt:
                                        continue
                        for item in history.get("items", []):
                            yielded += 1
                            if yielded <= 5:
                                logger.debug("yielding history: issue=%s field=%s created=%s", issue.get('key'), item.get('field'), created)
                            yield {
                                "issue_id": issue.get("id"),
                                "issue_key": issue.get("key"),
                                "history_created": created,
                                "history_author": author,
                                "field": item.get("field"),
                                "fromString": item.get("fromString"),
                                "toString": item.get("toString"),
                            }
        logger.debug("issue_histories yielded %s rows", yielded)
    resources.append(issue_histories)

    # issue_comments resource
    @dlt.resource(write_disposition="append")
    def issue_comments(
        subdomain: str = subdomain,
        email: str = email,
        api_token: str = api_token,
        page_size: int = page_size,
        jql_queries: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
    ):
        if jql_queries is None:
            jql_queries = DEFAULT_ENDPOINTS.get("issues", {}).get("params", {}).get("jql", [])
        if fields is None:
            fields = DEFAULT_ENDPOINTS.get("issues", {}).get("params", {}).get("fields", [])
        logger.debug("issue_comments resource called (per-issue API)")
        yielded = 0
        from .fetch_comments import fetch_issue_comments
        for jql in jql_queries if isinstance(jql_queries, list) else [jql_queries]:
            params = {"fields": fields, "jql": jql, "maxResults": page_size}
            for issues_page in get_paginated_data(
                api_path="rest/api/3/search/jql",
                data_path=None,
                subdomain=subdomain,
                email=email,
                api_token=api_token,
                page_size=page_size,
                params=params,
            ):
                for idx, issue in enumerate(issues_page):
                    if idx == 0:
                        import pprint
                        logger.debug("RAW ISSUE (issue_comments):\n%s", pprint.pformat(issue))
                    issue_id = issue.get("id")
                    issue_key = issue.get("key")
                    for comment in fetch_issue_comments(subdomain, email, api_token, issue_id):
                        yielded += 1
                        if yielded <= 5:
                            logger.debug("yielding comment: issue=%s comment_id=%s created=%s", issue_key, comment.get('id'), comment.get('created'))
                        yield {
                            "issue_id": issue_id,
                            "issue_key": issue_key,
                            "comment_id": comment.get("id"),
                            "author": comment.get("author", {}),
                            "created": comment.get("created"),
                            "updated": comment.get("updated"),
                            "body": comment.get("body"),
                        }
        logger.debug("issue_comments yielded %s rows", yielded)
    resources.append(issue_comments)

    # issue_sprints resource
    @dlt.resource(write_disposition="append")
    def issue_sprints(
        subdomain: str = subdomain,
        email: str = email,
        api_token: str = api_token,
        page_size: int = page_size,
        jql_queries: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
    ):
        if jql_queries is None:
            jql_queries = DEFAULT_ENDPOINTS.get("issues", {}).get("params", {}).get("jql", [])
        if fields is None:
            fields = DEFAULT_ENDPOINTS.get("issues", {}).get("params", {}).get("fields", [])
        logger.debug("issue_sprints resource called")
        yielded = 0
        from .extract_sprints import extract_issue_sprints
        for jql in jql_queries if isinstance(jql_queries, list) else [jql_queries]:
            params = {"fields": fields, "jql": jql, "maxResults": page_size}
            for issues_page in get_paginated_data(
                api_path="rest/api/3/search/jql",
                data_path=None,
                subdomain=subdomain,
                email=email,
                api_token=api_token,
                page_size=page_size,
                params=params,
            ):
                for idx, issue in enumerate(issues_page):
                    if idx == 0:
                        import pprint
                        logger.debug("RAW ISSUE (issue_sprints):\n%s", pprint.pformat(issue))
                    issue_id = issue.get("id")
                    issue_key = issue.get("key")
                    for sprint in extract_issue_sprints(issue):
                        yielded += 1
                        if yielded <= 5:
                            logger.debug("yielding sprint: issue=%s sprint_id=%s name=%s", issue_key, sprint.get('id'), sprint.get('name'))
                        yield {
                            "issue_id": issue_id,
                            "issue_key": issue_key,
                            **sprint
                        }
        logger.debug("issue_sprints yielded %s rows", yielded)
    resources.append(issue_sprints)

    # all_sprints resource
    @dlt.resource(name="all_sprints", write_disposition="replace")
    def all_sprints(
        subdomain: str = subdomain,
        email: str = email,
        api_token: str = api_token,
        board_type: Optional[str] = None,
    ):
        from .fetch_sprints import fetch_boards, fetch_sprints_for_board
        logger.debug("all_sprints resource called")
        yielded = 0
        for board in fetch_boards(subdomain, email, api_token, board_type):
            board_id = board.get("id")
            board_name = board.get("name")
            for sprint in fetch_sprints_for_board(subdomain, email, api_token, board_id):
                yielded += 1
                if yielded <= 5:
                    logger.debug("yielding sprint: board=%s board_id=%s sprint_id=%s name=%s", board_name, board_id, sprint.get('id'), sprint.get('name'))
                yield {
                    "board_id": board_id,
                    "board_name": board_name,
                    **sprint
                }
        logger.debug("all_sprints yielded %s rows", yielded)
    resources.append(all_sprints)

    return resources


@dlt.resource(write_disposition="replace")
def jira_issues(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    jql_queries: List[str],
    fields: List[str],
) -> Iterable[TDataItem]:
    for jql in jql_queries:
        params = {
            "fields": fields,
            "jql": jql,
            "maxResults": page_size,
        }
        yield from get_paginated_data(
            api_path="rest/api/3/search/jql",
            data_path=None,
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            page_size=page_size,
            params=params,
        )


def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    api_path: str,
    data_path: Optional[str],
    params: Optional[DictStrAny],
) -> Iterable[TDataItem]:
    """
    Fetch paginated data from Jira API.

    Supports both POST-based (search/jql) and GET-based endpoints.
    """
    url = f"https://{subdomain}.atlassian.net/{api_path}"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    auth = (email, api_token)
    params = {} if params is None else params

    if api_path.endswith("/search/jql"):
        # POST-based pagination for search/jql endpoint
        # This endpoint uses token-based pagination with nextPageToken

        # Extract request parameters
        jql = params.get("jql", "")
        fields = params.get("fields") or []
        expand = params.get("expand")

        # Normalize expand to comma-separated string
        expand_str = None
        if expand:
            expand_str = ",".join(expand) if isinstance(expand, (list, tuple)) else expand

        # Build base payload (don't include maxResults - let API use default per page)
        payload = {"jql": jql, "fields": fields}
        if expand_str:
            payload["expand"] = expand_str

        # Paginate through all results using token-based pagination
        next_page_token = None
        page_num = 0

        while True:
            page_num += 1

            # Add pagination token if we have one (for pages after the first)
            if next_page_token:
                payload["nextPageToken"] = next_page_token

            # Make request
            response = requests.post(url, auth=auth, headers=headers, json=clean_dict(payload))
            response.raise_for_status()
            data = response.json()

            # Extract and yield issues
            issues = data.get("issues") or []
            if not issues:
                logger.info(f"Page {page_num}: No issues returned, stopping pagination")
                break

            logger.info(f"Page {page_num}: Fetched {len(issues)} issues")
            yield issues

            # Check if this is the last page
            if data.get("isLast", False):
                logger.info(f"Page {page_num}: isLast=true, stopping pagination")
                break

            # Get token for next page
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                logger.info(f"Page {page_num}: No nextPageToken, stopping pagination")
                break

    else:
        # GET-based pagination for other endpoints
        # Uses traditional startAt/maxResults pagination

        start_at = 0
        max_results = page_size

        while True:
            # Update pagination params
            params["startAt"] = start_at
            params["maxResults"] = max_results

            # Make request
            response = requests.get(url, auth=auth, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract current page
            current_page = data.pop(data_path) if data_path else data
            if not current_page:
                break

            yield current_page

            # Check if more pages exist
            total = data.get("total")
            if total is None:
                break

            start_at += len(current_page) if isinstance(current_page, list) else 1
            if start_at >= total:
                break
