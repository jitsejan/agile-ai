select
    issue_key,
    assignee,
    status,
    days_in_status
from gold.ticket_aging
where status not in ('Done', 'Won''t Do', 'Closed', 'Cancelled')
order by days_in_status desc
limit 50;
