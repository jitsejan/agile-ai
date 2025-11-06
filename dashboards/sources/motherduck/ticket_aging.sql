select
    issue_key,
    assignee,
    status,
    days_in_status
from gold.ticket_aging
order by days_in_status desc
limit 50;
