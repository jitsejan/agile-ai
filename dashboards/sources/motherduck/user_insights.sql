select
    assignee,
    issues_assigned,
    issues_completed,
    round(100.0 * issues_completed / nullif(issues_assigned, 0), 1) as completion_pct
from gold.user_insights
order by issues_assigned desc;
