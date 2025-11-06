-- Gold model: Personal ticket status and health
-- Tracks assigned tickets, time in status, and alerts for action needed
with assigned_tickets as (
    select
        i.assignee,
        i.issue_id,
        i.issue_key,
        i.summary,
        i.status,
        i.status_category,
        i.priority,
        i.issue_type,
        i.created_at,
        i.updated_at,
        i.completed_at,
        try_cast(i.fields__customfield_10014 as double) as story_points,
        datediff('day', i.updated_at, current_timestamp) as days_since_last_update,
        datediff('day', i.created_at, current_timestamp) as age_days,
        case
            when i.status = 'Done' then 'Completed'
            when datediff('day', i.updated_at, current_timestamp) > 7 then 'Stale'
            when datediff('day', i.updated_at, current_timestamp) > 3 then 'At Risk'
            else 'Active'
        end as health_status
    from {{ ref('issues') }} i
    where i.assignee is not null
),
ticket_comments as (
    select
        issue_id,
        count(*) as comment_count,
        max(created) as last_comment_at
    from {{ source('jira', 'issue_comments') }}
    group by 1
),
ticket_status_changes as (
    select
        issue_id,
        count(*) as status_change_count,
        max(history_created) as last_status_change_at
    from {{ source('jira', 'issue_histories') }}
    where field = 'status'
    group by 1
)
select
    asgn.assignee,
    asgn.issue_id,
    asgn.issue_key,
    asgn.summary,
    asgn.status,
    asgn.status_category,
    asgn.priority,
    asgn.issue_type,
    asgn.created_at,
    asgn.updated_at,
    asgn.completed_at,
    asgn.story_points,
    asgn.days_since_last_update,
    asgn.age_days,
    asgn.health_status,
    coalesce(tc.comment_count, 0) as comment_count,
    tc.last_comment_at,
    datediff('day', tc.last_comment_at, current_timestamp) as days_since_last_comment,
    coalesce(tsc.status_change_count, 0) as status_change_count,
    tsc.last_status_change_at,
    datediff('day', tsc.last_status_change_at, current_timestamp) as days_since_status_change,
    -- Alert flags
    case when asgn.status != 'Done' and datediff('day', asgn.updated_at, current_timestamp) > 2 then 1 else 0 end as alert_no_update_2days,
    case when asgn.status != 'Done' and coalesce(tc.comment_count, 0) = 0 then 1 else 0 end as alert_no_comments,
    case when asgn.status = 'Done' and coalesce(tc.comment_count, 0) = 0 then 1 else 0 end as alert_closed_without_comment
from assigned_tickets asgn
left join ticket_comments tc on asgn.issue_id = tc.issue_id
left join ticket_status_changes tsc on asgn.issue_id = tsc.issue_id
order by asgn.assignee, asgn.health_status desc, asgn.days_since_last_update desc
