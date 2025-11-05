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
        res_function = dlt.resource(get_paginated_data, name=endpoint_name)(
            **new_endpoint_parameters,
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            page_size=page_size,
        )
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
    url = f"https://{subdomain}.atlassian.net/{api_path}"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    auth = (email, api_token)
    params = {} if params is None else params
    # Use Jira Cloud's typical pagination for the search/jql endpoint which
    # expects `startAt` and `maxResults` in the POST body. The previous
    # implementation used `pageToken`/`nextPageToken` which doesn't match the
    # Jira REST API and caused 400 responses.
    if api_path.endswith("/search/jql"):
        start_at = 0
        # helper to mask Authorization header when printing diagnostics
        def _mask_headers(hdrs: dict) -> dict:
            if not hdrs:
                return {}
            masked = {}
            for k, v in hdrs.items():
                if k.lower() == "authorization":
                    masked[k] = "*****MASKED*****"
                else:
                    masked[k] = v
            return masked
        # Try a few payload shapes to be compatible with tenant-specific
        # expectations for the /search/jql endpoint. We attempt variants in
        # order until one succeeds.
        while True:
            jql = params.get("jql", "")
            fields = params.get("fields") or []
            expand = params.get("expand")
            # normalize expand to a comma-separated string if it's a list
            if isinstance(expand, (list, tuple)):
                expand_str = ",".join(expand)
            else:
                expand_str = expand
            max_results = params.get("maxResults", page_size)

            payload_candidates = [
                # Bruno's working shape: jql + fields only (no pagination keys)
                {**({"jql": jql, "fields": fields} if not expand_str else {"jql": jql, "fields": fields, "expand": expand_str})},
                # v1: simple shape (jql, fields list, startAt/maxResults)
                {**({"jql": jql, "fields": fields, "startAt": start_at, "maxResults": max_results} if not expand_str else {"jql": jql, "fields": fields, "startAt": start_at, "maxResults": max_results, "expand": expand_str})},
                # v2: nested query object
                {**({"query": {"jql": jql, "fields": fields}, "startAt": start_at, "maxResults": max_results} if not expand_str else {"query": {"jql": jql, "fields": fields}, "startAt": start_at, "maxResults": max_results, "expand": expand_str})},
                # v3: nested query + separate fields key
                {**({"query": {"jql": jql}, "fields": fields, "startAt": start_at, "maxResults": max_results} if not expand_str else {"query": {"jql": jql}, "fields": fields, "startAt": start_at, "maxResults": max_results, "expand": expand_str})},
                # v4: fields as comma-separated string
                {**({"jql": jql, "fields": ",".join(fields) if fields else [], "startAt": start_at, "maxResults": max_results} if not expand_str else {"jql": jql, "fields": ",".join(fields) if fields else [], "startAt": start_at, "maxResults": max_results, "expand": expand_str})},
                # v5: nested page object
                {**({"query": {"jql": jql}, "fields": fields, "page": {"startAt": start_at, "maxResults": max_results}} if not expand_str else {"query": {"jql": jql}, "fields": fields, "page": {"startAt": start_at, "maxResults": max_results}, "expand": expand_str})},
                # v6: nested query with paging inside
                {**({"query": {"jql": jql, "startAt": start_at, "maxResults": max_results}, "fields": fields} if not expand_str else {"query": {"jql": jql, "startAt": start_at, "maxResults": max_results}, "fields": fields, "expand": expand_str})},
                # v7: use 'limit' instead of maxResults
                {**({"jql": jql, "fields": fields, "startAt": start_at, "limit": max_results} if not expand_str else {"jql": jql, "fields": fields, "startAt": start_at, "limit": max_results, "expand": expand_str})},
                # v8: query/jql as nested and fields as comma string
                {**({"query": {"jql": jql}, "fields": ",".join(fields) if fields else [], "startAt": start_at, "maxResults": max_results} if not expand_str else {"query": {"jql": jql}, "fields": ",".join(fields) if fields else [], "startAt": start_at, "maxResults": max_results, "expand": expand_str})},
            ]

            success = False
            last_exception = None
            for payload in payload_candidates:
                try:
                    response = requests.post(url, auth=auth, headers=headers, json=payload)

                    # Try to extract the prepared request to print diagnostics
                    req = getattr(response, "request", None)
                    if req is not None:
                        try:
                            req_headers = dict(req.headers)
                        except Exception:
                            req_headers = {}
                        masked_req_headers = _mask_headers(req_headers)
                        req_body = getattr(req, "body", None)
                        # If body is bytes, try to decode for readability
                        if isinstance(req_body, (bytes, bytearray)):
                            try:
                                req_body = req_body.decode("utf-8")
                            except Exception:
                                req_body = str(req_body)

                        logger.debug("REQUEST -> %s %s", req.method if hasattr(req, 'method') else "POST", req.url if hasattr(req, 'url') else url)
                        logger.debug("REQUEST HEADERS -> %s", masked_req_headers)
                        logger.debug("REQUEST BODY -> %s", req_body if req_body is not None else payload)

                    # Raise for status so we consistently handle non-2xx responses
                    response.raise_for_status()
                    data = response.json()
                    success = True
                    break
                except Exception as ex:
                    # Log attempt and reason, then try next candidate.
                    resp = getattr(ex, "response", None)
                    resp_text = None
                    status = None
                    try:
                        if resp is not None:
                            status = resp.status_code
                            resp_text = resp.text
                    except Exception:
                        resp_text = "<no response body>"

                    # Attempt to get request from response or from the exception
                    sent_req = None
                    if resp is not None:
                        sent_req = getattr(resp, "request", None)
                    if sent_req is None:
                        sent_req = getattr(ex, "request", None)

                    if sent_req is not None:
                        try:
                            sent_headers = dict(sent_req.headers)
                        except Exception:
                            sent_headers = {}
                        masked_sent_headers = _mask_headers(sent_headers)
                        sent_body = getattr(sent_req, "body", None)
                        if isinstance(sent_body, (bytes, bytearray)):
                            try:
                                sent_body = sent_body.decode("utf-8")
                            except Exception:
                                sent_body = str(sent_body)
                        logger.debug("TRIED PAYLOAD -> %s", payload)
                        logger.debug("SENT REQUEST HEADERS -> %s", masked_sent_headers)
                        logger.debug("SENT REQUEST BODY -> %s", sent_body if sent_body is not None else payload)
                    else:
                        logger.debug("TRIED PAYLOAD -> %s", payload)

                    logger.debug("RESPONSE STATUS -> %s", status)
                    logger.debug("RESPONSE BODY -> %s", resp_text if resp_text is not None else "<no response body>")
                    last_exception = ex
                    continue

            if not success:
                # Re-raise the last encountered exception so callers see the error
                if last_exception is not None:
                    raise last_exception
                break

            # Process the successful response. The Bruno-compatible shape
            # (jql + fields) may return paged results. We'll yield the
            # returned issues and, if `total` indicates more pages, we'll
            # fetch them by sending startAt/maxResults as query params on
            # subsequent POSTs (keeping the JSON body minimal). This avoids
            # the tenant rejecting startAt when present inside the JSON body.
            issues = data.get("issues") or []
            if not issues:
                break

            # Yield the list of issues for this page
            yield issues

            # Pagination: determine if there are more issues to fetch
            fetched = len(issues)
            total = data.get("total")

            # If there's no total or we've fetched all, stop
            if total is None or fetched == 0:
                break

            start_at = int(data.get("startAt", start_at))
            next_start = start_at + fetched
            if next_start >= int(total):
                break

            # Fetch subsequent pages. Use query params for startAt/maxResults
            # to avoid placing them in the JSON body which this tenant rejects.
            while next_start < int(total):
                # Prepare a minimal body (Bruno shape). Include expand if requested.
                page_body = {"jql": jql, "fields": fields}
                if expand_str:
                    page_body["expand"] = expand_str
                # Place paging into query params to avoid tenant rejection when
                # present inside JSON body; place expand into params too.
                page_params = {"startAt": next_start, "maxResults": max_results}
                if expand_str:
                    page_params["expand"] = expand_str
                try:
                    resp = requests.post(url, auth=auth, headers=headers, params=page_params, json=page_body)
                    # expose prepared request diagnostics when debug is present
                    req = getattr(resp, "request", None)
                    if req is not None:
                        try:
                            req_headers = dict(req.headers)
                        except Exception:
                            req_headers = {}
                        masked_req_headers = _mask_headers(req_headers)
                        req_body = getattr(req, "body", None)
                        if isinstance(req_body, (bytes, bytearray)):
                            try:
                                req_body = req_body.decode("utf-8")
                            except Exception:
                                req_body = str(req_body)
                        logger.debug("PAGED REQUEST -> %s %s", req.method if hasattr(req, 'method') else "POST", req.url if hasattr(req, 'url') else url)
                        logger.debug("PAGED REQUEST HEADERS -> %s", masked_req_headers)
                        logger.debug("PAGED REQUEST BODY -> %s", req_body if req_body is not None else page_body)

                    resp.raise_for_status()
                except Exception as ex:
                    # Try a fallback: some tenants do accept startAt/maxResults
                    # in the JSON body. Attempt it once before raising.
                    try:
                        fallback_body = {"jql": jql, "fields": fields, "startAt": next_start, "maxResults": max_results}
                        logger.debug("PAGED FALLBACK BODY -> %s", fallback_body)
                        resp = requests.post(url, auth=auth, headers=headers, json=fallback_body)
                        resp.raise_for_status()
                    except Exception:
                        # Give up and surface the original exception
                        raise ex

                page_data = resp.json()
                page_issues = page_data.get("issues") or []
                if not page_issues:
                    break
                yield page_issues

                fetched = len(page_issues)
                start_at = int(page_data.get("startAt", next_start))
                next_start = start_at + fetched
                if next_start >= int(page_data.get("total", total)):
                    break
    else:
        # Non-search endpoints use standard GET with query params
        while True:
            try:
                response = requests.get(url, auth=auth, headers=headers, params=params)
            except requests.HTTPError as ex:
                resp = getattr(ex, "response", None)
                resp_text = resp.text if resp is not None else "<no response body>"
                status = resp.status_code if resp is not None else "<no status>"
                print("[jira] GET params:", params)
                print("[jira] response status:", status)
                print("[jira] response body:", resp_text)
                raise

            data = response.json()
            current_page = data.pop(data_path) if data_path else data
            if not current_page:
                break
            yield current_page
