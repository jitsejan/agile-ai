-- Gold model: User insights (issues assigned, completed)
select
    assignee,
    count(distinct issue_id) as issues_assigned,
    count(distinct case when completed_at is not null then issue_id end) as issues_completed
from {{ ref('silver_issues') }}
group by assignee
order by issues_assigned desc
