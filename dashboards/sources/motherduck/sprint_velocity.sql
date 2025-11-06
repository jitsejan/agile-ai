select
    sprint_name,
    start_date,
    end_date,
    issues_committed,
    issues_completed,
    completion_ratio
from gold.sprint_velocity
order by start_date desc
limit 20;
