with base as (
    select
        id as issue_id,
        key as issue_key,
        fields__summary as summary,
        fields__status->>'name' as status,
        fields__status->>'statusCategory' as status_category,
        fields__assignee->>'displayName' as assignee,
        fields__created as created_at,
        fields__updated as updated_at,
        fields__priority->>'name' as priority,
        fields__reporter->>'displayName' as reporter,
        fields__issuetype->>'name' as issue_type,
        fields__labels as labels_raw,
        fields__customfield_10020 as sprint_raw,
        fields__customfield_10014 as fields__customfield_10014
    from {{ source('jira', 'issues') }}
)
select
    *,
    case when status = 'Done' then updated_at end as completed_at
from base
