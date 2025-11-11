-- Gold model: Personal activity tracking
-- Monitors comment activity, ticket updates, and engagement
with comment_activity as (
    select
        author__display_name as user_name,
        date_trunc('day', created) as activity_date,
        count(distinct comment_id) as comments_made,
        count(distinct issue_id) as issues_commented_on
    from {{ source('jira', 'issue_comments') }}
    where author__display_name is not null
    group by 1, 2
),
update_activity as (
    select
        history_author__display_name as user_name,
        date_trunc('day', history_created) as activity_date,
        count(*) as updates_made,
        count(distinct issue_id) as issues_updated
    from {{ source('jira', 'issue_histories') }}
    where history_author__display_name is not null
    group by 1, 2
),
all_dates as (
    select distinct activity_date
    from (
        select activity_date from comment_activity
        union all
        select activity_date from update_activity
    )
),
all_users as (
    select distinct user_name
    from (
        select user_name from comment_activity
        union all
        select user_name from update_activity
    )
),
user_date_skeleton as (
    select u.user_name, d.activity_date
    from all_users u
    cross join all_dates d
)
select
    uds.user_name,
    uds.activity_date,
    coalesce(ca.comments_made, 0) as comments_made,
    coalesce(ca.issues_commented_on, 0) as issues_commented_on,
    coalesce(ua.updates_made, 0) as updates_made,
    coalesce(ua.issues_updated, 0) as issues_updated,
    coalesce(ca.comments_made, 0) + coalesce(ua.updates_made, 0) as total_activities,
    -- Days since last activity
    datediff('day', uds.activity_date, current_date) as days_since_activity,
    -- Flag for inactivity (no activity on this day)
    case when coalesce(ca.comments_made, 0) + coalesce(ua.updates_made, 0) = 0 then 1 else 0 end as inactive_day
from user_date_skeleton uds
left join comment_activity ca
    on uds.user_name = ca.user_name
    and uds.activity_date = ca.activity_date
left join update_activity ua
    on uds.user_name = ua.user_name
    and uds.activity_date = ua.activity_date
where uds.activity_date >= current_date - interval '90 days'
order by uds.user_name, uds.activity_date desc
