-- Gold model: Sprint velocity per sprint (issue counts)
-- Filtered to board_id = 70 (Data Team Board)
with sprint_issues as (
    select
        s.sprint_id,
        s.sprint_name,
        s.board_id,
        s.start_date,
        s.end_date,
        i.issue_id,
        i.summary,
        i.status,
        i.completed_at,
        case when i.completed_at is not null then 1 else 0 end as completed_flag
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
    count(issue_id) as issues_committed,
    coalesce(sum(completed_flag), 0) as issues_completed,
    round(coalesce(sum(completed_flag), 0) * 1.0 / nullif(count(issue_id), 0), 2) as completion_ratio
from sprint_issues
group by 1,2,3,4,5
order by start_date desc
