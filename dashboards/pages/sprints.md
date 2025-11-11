---
title: Sprint Analytics
max_width: 1920px
---

# 📈 Sprint Performance Analysis

```sql sprint_perf
select *
from motherduck.sprint_performance
order by start_date asc;
```

```sql sprint_vel
select *
from motherduck.sprint_velocity
order by start_date asc;
```

```sql carryover
select *
from motherduck.sprint_carryover
order by from_sprint_number desc, changed_at desc;
```

## Key Metrics

```sql sprint_kpis
select
  count(*) as total_sprints,
  round(avg(completion_ratio) * 100, 1) as avg_completion_rate,
  round(avg(issues_completed), 1) as avg_velocity,
  max(issues_completed) as best_velocity
from motherduck.sprint_velocity;
```

<Grid cols=4>
  <BigValue
    data={sprint_kpis}
    value=total_sprints
    title="Total Sprints"
  />
  <BigValue
    data={sprint_kpis}
    value=avg_completion_rate
    title="Avg Completion"
    fmt='0.0"%"'
  />
  <BigValue
    data={sprint_kpis}
    value=avg_velocity
    title="Avg Velocity"
    fmt='0.0'
  />
  <BigValue
    data={sprint_kpis}
    value=best_velocity
    title="Best Velocity"
  />
</Grid>

---

## 📊 Velocity Trends

<LineChart
  data={sprint_vel}
  x=sprint_name
  y={['issues_committed', 'issues_completed']}
  yAxisTitle="Issues"
  title="Sprint Velocity Over Time"
  markers=true
  sort=false
/>

<BarChart
  data={sprint_vel}
  x=sprint_name
  y=completion_ratio
  yFmt='0%'
  title="Completion Rate by Sprint"
  sort=false
  swapXY=true
/>

---

## 🔄 Sprint Carryover Analysis

```sql carryover_by_sprint
select
  c.from_sprint as sprint_name,
  count(distinct c.issue_id) as carryover_count,
  count(distinct c.issue_key) as issues_moved,
  sum(c.story_points) as total_story_points,
  min(s.start_date) as start_date
from motherduck.sprint_carryover c
left join motherduck.sprint_performance s on c.from_sprint = s.sprint_name
where c.from_sprint is not null
group by c.from_sprint
order by start_date asc
```

```sql carryover_summary
select
  case
    when carryover_count = 0 then 'No Carryover'
    when carryover_count <= 3 then 'Low (1-3)'
    when carryover_count <= 6 then 'Medium (4-6)'
    else 'High (>6)'
  end as carryover_category,
  count(*) as sprint_count
from ${carryover_by_sprint}
group by 1
order by case
  when carryover_category = 'No Carryover' then 0
  when carryover_category = 'Low (1-3)' then 1
  when carryover_category = 'Medium (4-6)' then 2
  else 3
end;
```

<BarChart
  data={carryover_summary}
  x=carryover_category
  y=sprint_count
  title="Sprints by Carryover Level"
/>

### Recent Sprint Carryover

<DataTable data={carryover_by_sprint} rows=15>
  <Column id=sprint_name/>
  <Column id=carryover_count contentType=colorscale scaleColor=red/>
  <Column id=issues_moved contentType=colorscale scaleColor=orange/>
  <Column id=total_story_points fmt='0.0'/>
</DataTable>

---

## 📋 Detailed Sprint Performance

<DataTable data={sprint_perf} search=true>
  <Column id=sprint_name/>
  <Column id=start_date/>
  <Column id=end_date/>
  <Column id=issues_committed fmt='#,##0'/>
  <Column id=issues_completed fmt='#,##0'/>
  <Column id=completion_ratio fmt='0%' contentType=colorscale scaleColor=green/>
</DataTable>
