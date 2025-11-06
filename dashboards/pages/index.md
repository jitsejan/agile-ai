---
title: Jira Delivery Dashboard
---

This dashboard visualizes the gold models stored in MotherDuck.

```sql user_insights
select
  assignee,
  issues_assigned,
  issues_completed,
  round(100.0 * issues_completed / nullif(issues_assigned, 0), 1) as completion_pct
from motherduck.user_insights
order by issues_assigned desc;
```

```sql ticket_aging
select
  issue_key,
  assignee,
  status,
  days_in_status
from motherduck.ticket_aging
order by days_in_status desc
limit 50;
```

```sql sprint_velocity
select
  sprint_name,
  start_date,
  end_date,
  issues_committed,
  issues_completed,
  completion_ratio
from motherduck.sprint_velocity
order by start_date desc
limit 20;
```

## Team Throughput

<DataTable data={user_insights} />

## Longest-Running Tickets

<DataTable data={ticket_aging} />

## Sprint Velocity

<BarChart
    data={sprint_velocity}
    x=sprint_name
    y=issues_completed
    series=issues_committed
    title="Completed vs Committed"
/>
