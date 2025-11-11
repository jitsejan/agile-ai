---
title: Team Performance
max_width: 1920px
---

# 👥 Team Performance Metrics

```sql team_perf
select *
from motherduck.team_member_performance
order by assignee;
```

```sql user_insights
select *
from motherduck.user_insights
where assignee is not null
order by issues_assigned desc;
```

## Team Overview

```sql team_kpis
select
  count(distinct assignee) as team_size,
  sum(issues_assigned) as total_workload,
  round(avg(issues_assigned), 1) as avg_per_person,
  round(100.0 * sum(issues_completed) / nullif(sum(issues_assigned), 0), 1) as team_completion_rate
from motherduck.user_insights
where assignee is not null;
```

<Grid cols=4>
  <BigValue
    data={team_kpis}
    value=team_size
    title="Team Size"
  />
  <BigValue
    data={team_kpis}
    value=total_workload
    title="Total Issues"
    fmt='#,##0'
  />
  <BigValue
    data={team_kpis}
    value=avg_per_person
    title="Avg per Person"
    fmt='0.0'
  />
  <BigValue
    data={team_kpis}
    value=team_completion_rate
    title="Team Completion"
    fmt='0.0"%"'
  />
</Grid>

---

## 📊 Individual Performance

```sql completion_rates
select
  assignee,
  issues_assigned,
  issues_completed,
  round(100.0 * issues_completed / nullif(issues_assigned, 0), 1) as completion_rate
from motherduck.user_insights
where assignee is not null
order by completion_rate desc;
```

```sql workload_buckets
select
  case
    when issues_assigned < 5 then '< 5 issues'
    when issues_assigned < 10 then '5-9 issues'
    when issues_assigned < 20 then '10-19 issues'
    else '20+ issues'
  end as workload_bucket,
  count(*) as team_members,
  sum(issues_assigned) as total_issues
from motherduck.user_insights
where assignee is not null
group by 1
order by min(issues_assigned);
```

<Grid cols=2>
  <div>
    <BarChart
      data={user_insights}
      x=assignee
      y={['issues_assigned', 'issues_completed']}
      swapXY=true
      title="Workload vs Completion"
    />
  </div>
  <div>
    <BarChart
      data={completion_rates}
      x=assignee
      y=completion_rate
      yFmt='0"%"'
      swapXY=true
      title="Completion Rate"
      colorPalette={['#16a34a', '#84cc16', '#eab308', '#f97316', '#dc2626']}
    />
  </div>
</Grid>

<Grid cols=2>
  <div>
    <BarChart
      data={workload_buckets}
      x=workload_bucket
      y=team_members
      title="Team Members by Workload"
    />
  </div>
  <div>
    ### Team Performance Details
    <DataTable data={user_insights} search=true rows=10>
      <Column id=assignee/>
      <Column id=issues_assigned fmt='#,##0' contentType=colorscale scaleColor=blue/>
      <Column id=issues_completed fmt='#,##0' contentType=colorscale scaleColor=green/>
    </DataTable>
  </div>
</Grid>

---

## 🏆 Performance Highlights

```sql top_performers
select
  assignee,
  issues_completed,
  round(100.0 * issues_completed / nullif(issues_assigned, 0), 1) as completion_rate
from motherduck.user_insights
where assignee is not null
order by issues_completed desc
limit 5;
```

```sql needs_attention
select
  assignee,
  issues_assigned,
  issues_completed,
  (issues_assigned - issues_completed) as backlog,
  round(100.0 * issues_completed / nullif(issues_assigned, 0), 1) as completion_rate
from motherduck.user_insights
where assignee is not null
  and (issues_assigned - issues_completed) > 5
order by backlog desc;
```

<Grid cols=2>
  <div>
    ### 🏆 Top Performers
    <DataTable data={top_performers}>
      <Column id=assignee/>
      <Column id=issues_completed fmt='#,##0'/>
      <Column id=completion_rate fmt='0.0"%"' contentType=colorscale scaleColor=green/>
    </DataTable>
  </div>
  <div>
    ### 🔴 Attention Needed
    <DataTable data={needs_attention}>
      <Column id=assignee/>
      <Column id=issues_assigned fmt='#,##0'/>
      <Column id=issues_completed fmt='#,##0'/>
      <Column id=backlog fmt='#,##0' contentType=colorscale scaleColor=red/>
      <Column id=completion_rate fmt='0.0"%"'/>
    </DataTable>
  </div>
</Grid>
