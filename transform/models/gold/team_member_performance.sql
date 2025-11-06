-- Gold model: Individual team member performance across sprints
-- Tracks velocity, completion rate, and activity per person
with sprint_issues as (
    select
        s.sprint_id,
        s.sprint_name,
        s.start_date,
        s.end_date,
        i.assignee,
        i.issue_id,
        i.issue_key,
        i.status,
        i.completed_at,
        try_cast(i.fields__customfield_10014 as double) as story_points,
        case when i.completed_at is not null then 1 else 0 end as completed_flag,
        case
            when i.completed_at is not null
                and i.completed_at >= s.start_date
                and i.completed_at <= s.end_date
            then 1
            else 0
        end as completed_in_sprint_flag,
        datediff('day', i.created_at, coalesce(i.completed_at, current_timestamp)) as days_to_complete
    from {{ ref('issues') }} i
    join {{ ref('sprints') }} s
      on cast(i.sprint_raw as varchar) like '%' || cast(s.sprint_id as varchar) || '%'
)
select
    sprint_id,
    sprint_name,
    start_date,
    end_date,
    assignee,
    count(distinct issue_id) as issues_assigned,
    sum(completed_flag) as issues_completed,
    sum(completed_in_sprint_flag) as issues_completed_in_sprint,
    coalesce(sum(story_points), 0) as points_assigned,
    coalesce(sum(case when completed_flag = 1 then story_points end), 0) as points_completed,
    coalesce(sum(case when completed_in_sprint_flag = 1 then story_points end), 0) as points_completed_in_sprint,
    round(sum(completed_flag) * 100.0 / nullif(count(issue_id), 0), 2) as completion_rate_pct,
    round(avg(case when completed_flag = 1 then days_to_complete end), 1) as avg_days_to_complete
from sprint_issues
where assignee is not null
group by 1,2,3,4,5
order by start_date desc, points_completed desc
