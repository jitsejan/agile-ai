-- Gold model: Comprehensive sprint performance metrics
-- Tracks story points, completion rates, and sprint health
-- Filtered to board_id = 70 (Data Team Board)
with sprint_issues as (
    select
        s.sprint_id,
        s.sprint_name,
        s.board_id,
        s.start_date,
        s.end_date,
        s.sprint_state,
        i.issue_id,
        i.issue_key,
        i.summary,
        i.status,
        i.assignee,
        i.completed_at,
        -- Extract story points from customfield_10014
        try_cast(i.fields__customfield_10014 as double) as story_points,
        case when i.completed_at is not null then 1 else 0 end as completed_flag,
        case
            when i.completed_at is not null
                and i.completed_at >= s.start_date
                and i.completed_at <= s.end_date
            then 1
            else 0
        end as completed_in_sprint_flag
    from {{ ref('sprints') }} s
    left join {{ ref('issues') }} i
      on cast(i.sprint_raw as varchar) like '%' || cast(s.sprint_id as varchar) || '%'
    where s.board_id = 70
)
select
    sprint_id,
    sprint_name,
    board_id,
    start_date,
    end_date,
    sprint_state,
    datediff('day', start_date, end_date) as sprint_duration_days,
    count(distinct issue_id) as issues_committed,
    sum(completed_flag) as issues_completed,
    sum(completed_in_sprint_flag) as issues_completed_in_sprint,
    count(distinct issue_id) - sum(completed_flag) as issues_remaining,
    coalesce(sum(story_points), 0) as points_committed,
    coalesce(sum(case when completed_flag = 1 then story_points end), 0) as points_completed,
    coalesce(sum(case when completed_in_sprint_flag = 1 then story_points end), 0) as points_completed_in_sprint,
    coalesce(sum(story_points), 0) - coalesce(sum(case when completed_flag = 1 then story_points end), 0) as points_remaining,
    round(sum(completed_flag) * 100.0 / nullif(count(issue_id), 0), 2) as completion_rate_pct,
    round(coalesce(sum(case when completed_flag = 1 then story_points end), 0) * 100.0 / nullif(sum(story_points), 0), 2) as points_completion_rate_pct,
    count(distinct assignee) as team_members_active
from sprint_issues
group by 1,2,3,4,5,6
order by start_date desc
