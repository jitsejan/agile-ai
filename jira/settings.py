DEFAULT_ENDPOINTS = {
    "issues": {
        "data_path": None,
    # Use the newer search/jql endpoint (expects POST).
    # NOTE: This endpoint uses token-based pagination with a 100-page limit
    "api_path": "rest/api/3/search/jql",
        "params": {
            # Expanded default fields to include typical fields useful for
            # historical analysis (timestamps, reporters, custom fields,
            # and sprint-related fields). You can add or remove fields as
            # needed per tenant.
            "fields": [
                "id",
                "key",
                "summary",
                "description",
                "issuetype",
                "status",
                "resolution",
                "assignee",
                "reporter",
                "priority",
                "labels",
                "components",
                "created",
                "updated",
                "resolutiondate",
                "duedate",
                "lastViewed",
                "customfield_10016",
                "customfield_10020",
                "closedSprints",
                "customfield_10014",
            ],
            # Request the changelog expansion so we can analyze field history
            # for tickets. Some tenants require `expand=changelog` as a
            # query parameter rather than in the JSON body — `get_paginated_data`
            # will place it appropriately.
            "expand": ["changelog"],
            # Default JQL: query project DT and order by most recently created
            "jql": "project = DT ORDER BY created DESC",
            # NOTE: Omitting maxResults allows proper pagination with isLast flag
        },
    },
}

DEFAULT_PAGE_SIZE = 100
