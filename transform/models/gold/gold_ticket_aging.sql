-- Gold model: Aging open tickets
select
    issue_id,
    issue_key,
    summary,
    status,
    assignee,
    created_at,
    completed_at,
    datediff('day', created_at, coalesce(completed_at, current_timestamp)) as days_in_status
from {{ ref('silver_issues') }}
order by days_in_status desc
