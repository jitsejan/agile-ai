-- Gold model: Sprint velocity per sprint (issue counts)
with sprint_issues as (
    select
        s.sprint_id,
        s.sprint_name,
        s.start_date,
        s.end_date,
        i.issue_id,
        i.summary,
        i.status,
        i.completed_at,
        case when i.completed_at is not null then 1 else 0 end as completed_flag
    from {{ ref('silver_issues') }} i
    join {{ ref('silver_sprints') }} s
      on cast(i.sprint_raw as varchar) like '%' || cast(s.sprint_id as varchar) || '%'
)
select
    sprint_id,
    sprint_name,
    start_date,
    end_date,
    count(issue_id) as issues_committed,
    sum(completed_flag) as issues_completed,
    round(sum(completed_flag) * 1.0 / nullif(count(issue_id), 0), 2) as completion_ratio
from sprint_issues
group by 1,2,3,4
order by start_date desc
