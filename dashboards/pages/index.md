---
title: Jira Analytics Dashboard
max_width: 1920px
---

# 📊 Executive Overview

```sql kpis
select
  sum(issues_assigned) as total_issues,
  sum(issues_completed) as completed_issues,
  round(100.0 * sum(issues_completed) / nullif(sum(issues_assigned), 0), 1) as overall_completion_rate,
  count(distinct assignee) as active_team_members
from motherduck.user_insights
where assignee is not null;
```

```sql sprint_summary
select
  count(*) as total_sprints,
  round(avg(completion_ratio) * 100, 1) as avg_completion_rate,
  round(avg(issues_completed), 1) as avg_velocity
from motherduck.sprint_velocity;
```

<Grid cols=4>
  <BigValue
    data={kpis}
    value=total_issues
    title="Total Issues"
    fmt='#,##0'
  />
  <BigValue
    data={kpis}
    value=completed_issues
    title="Completed"
    fmt='#,##0'
  />
  <BigValue
    data={kpis}
    value=overall_completion_rate
    title="Completion Rate"
    fmt='0.0"%"'
  />
  <BigValue
    data={kpis}
    value=active_team_members
    title="Team Members"
    fmt='#,##0'
  />
</Grid>

---

## 🎯 Sprint Velocity Trend

<Dropdown name=sprint_count title="Number of Sprints" defaultValue="10">
  <DropdownOption value="3" valueLabel="Last 3 Sprints"/>
  <DropdownOption value="5" valueLabel="Last 5 Sprints"/>
  <DropdownOption value="10" valueLabel="Last 10 Sprints"/>
  <DropdownOption value="20" valueLabel="Last 20 Sprints"/>
  <DropdownOption value="all" valueLabel="All Sprints"/>
</Dropdown>

```sql sprint_trend
with recent_sprints as (
  select
    sprint_name,
    start_date,
    end_date,
    issues_committed,
    issues_completed,
    round(completion_ratio * 100, 1) as completion_pct
  from motherduck.sprint_velocity
  order by end_date DESC
  limit case when '${inputs.sprint_count.value}' = 'all' then 1000 else cast('${inputs.sprint_count.value}' as integer) end
)
select * from recent_sprints
order by start_date asc;
```

<Grid cols=2>
  <div>
    <LineChart
      data={sprint_trend}
      x=sprint_name
      y=issues_completed
      yAxisTitle="Issues"
      title="Sprint Completion Trend"
      sort=false
    />
  </div>
  <div>
    <BarChart
      data={sprint_trend}
      x=sprint_name
      y={['issues_committed', 'issues_completed']}
      swapXY=true
      title="Committed vs Completed by Sprint"
      sort=false
    />
  </div>
</Grid>

---

## 👥 Team Performance

<Dropdown name=sprint_filter>
  <DropdownOption value="all_time" valueLabel="All Time"/>
  <DropdownOption value="current_sprint" valueLabel="Current Sprint Only"/>
</Dropdown>

```sql current_sprint_id
select sprint_id, sprint_name
from motherduck.sprint_velocity
order by start_date desc
limit 1;
```

```sql team_perf
select
  assignee,
  issues_assigned,
  issues_completed,
  round(100.0 * issues_completed / nullif(issues_assigned, 0), 1) as completion_pct
from motherduck.user_insights
where assignee is not null
  and (
    '${inputs.sprint_filter}' = 'all_time'
    or 1=1  -- Will be replaced with sprint filter logic
  )
order by issues_assigned desc;
```

```sql team_perf_sprint
select
  assignee,
  sum(issues_assigned) as issues_assigned,
  sum(issues_completed) as issues_completed,
  round(100.0 * sum(issues_completed) / nullif(sum(issues_assigned), 0), 1) as completion_pct
from motherduck.team_member_performance
where sprint_id = (select sprint_id from ${current_sprint_id})
  and assignee is not null
group by assignee
order by issues_assigned desc;
```

{#if inputs.sprint_filter === 'current_sprint'}
<Grid cols=2>
  <div>
    <BarChart
      data={team_perf_sprint}
      x=assignee
      y=issues_completed
      swapXY=true
      title="Issues Completed (Current Sprint: {current_sprint_id[0].sprint_name})"
    />
  </div>
  <div>
    ### Team Member Details
    <DataTable data={team_perf_sprint} rows=10>
      <Column id=assignee/>
      <Column id=issues_assigned fmt='#,##0'/>
      <Column id=issues_completed fmt='#,##0'/>
      <Column id=completion_pct fmt='0.0"%"' contentType=colorscale scaleColor=green/>
    </DataTable>
  </div>
</Grid>
{:else}
<Grid cols=2>
  <div>
    <BarChart
      data={team_perf}
      x=assignee
      y=issues_completed
      swapXY=true
      title="Issues Completed by Team Member (All Time)"
    />
  </div>
  <div>
    ### Team Member Details
    <DataTable data={team_perf} rows=10>
      <Column id=assignee/>
      <Column id=issues_assigned fmt='#,##0'/>
      <Column id=issues_completed fmt='#,##0'/>
      <Column id=completion_pct fmt='0.0"%"' contentType=colorscale scaleColor=green/>
    </DataTable>
  </div>
</Grid>
{/if}

---

## ⏰ Ticket Aging Analysis

```sql aging_summary
select
  case
    when days_in_status < 7 then '< 1 week'
    when days_in_status < 30 then '1-4 weeks'
    when days_in_status < 90 then '1-3 months'
    else '> 3 months'
  end as age_bucket,
  count(*) as ticket_count,
  round(avg(days_in_status), 1) as avg_days
from motherduck.ticket_aging
group by 1
order by min(days_in_status);
```

```sql top_aging
select
  t.issue_key,
  c.jira_base_url || '/browse/' || t.issue_key as issue_url,
  t.assignee,
  t.status,
  t.days_in_status
from motherduck.ticket_aging t
left join motherduck.jira_config c
  on split_part(t.issue_key, '-', 1) = c.project_key
order by t.days_in_status desc
limit 10;
```

<Grid cols=2>
  <div>
    <BarChart
      data={aging_summary}
      x=age_bucket
      y=ticket_count
      title="Tickets by Age"
    />
  </div>
  <div>
    ### Top 10 Longest Running Tickets
    <DataTable data={top_aging}>
      <Column id=issue_url title="Issue Key" contentType=link linkLabel=issue_key openInNewTab=true/>
      <Column id=assignee/>
      <Column id=status/>
      <Column id=days_in_status fmt='#,##0' contentType=colorscale scaleColor=red/>
    </DataTable>
  </div>
</Grid>

