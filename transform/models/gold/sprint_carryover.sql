-- Gold model: Track tickets that moved between sprints (carryover/spillover)
-- Helps identify estimation issues and scope creep
with issue_sprint_changes as (
    select
        h.issue_id,
        h.issue_key,
        h.history_created as changed_at,
        h.history_author__display_name as changed_by,
        h.from_string as from_sprint,
        h.to_string as to_sprint
    from {{ source('jira', 'issue_histories') }} h
    where h.field = 'Sprint'
        and h.from_string is not null
        and h.to_string is not null
),
issue_details as (
    select
        i.issue_id,
        i.issue_key,
        i.summary,
        i.status,
        i.assignee,
        try_cast(i.fields__customfield_10014 as double) as story_points
    from {{ ref('issues') }} i
)
select
    sc.issue_id,
    sc.issue_key,
    id.summary,
    id.status,
    id.assignee,
    id.story_points,
    sc.changed_at,
    sc.changed_by,
    sc.from_sprint,
    sc.to_sprint,
    count(*) over (partition by sc.issue_id) as times_moved
from issue_sprint_changes sc
left join issue_details id on sc.issue_id = id.issue_id
order by sc.changed_at desc
