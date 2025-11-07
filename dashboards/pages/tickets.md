---
title: Ticket Analysis
---

# 🎫 Ticket Insights & Aging

```sql ticket_aging
select *
from motherduck.ticket_aging
order by days_in_status desc;
```

## Aging Overview

```sql aging_kpis
select
  count(*) as total_tickets,
  round(avg(days_in_status), 1) as avg_age,
  max(days_in_status) as oldest_ticket,
  count(case when days_in_status > 90 then 1 end) as critical_aging
from motherduck.ticket_aging;
```

<Grid cols=4>
  <BigValue
    data={aging_kpis}
    value=total_tickets
    title="Total Tickets"
    fmt='#,##0'
  />
  <BigValue
    data={aging_kpis}
    value=avg_age
    title="Avg Age (days)"
    fmt='0.0'
  />
  <BigValue
    data={aging_kpis}
    value=oldest_ticket
    title="Oldest Ticket"
    fmt='#,##0'
  />
  <BigValue
    data={aging_kpis}
    value=critical_aging
    title="Critical (>90d)"
    fmt='#,##0'
  />
</Grid>

---

## 📊 Age Distribution

```sql age_buckets
select
  case
    when days_in_status < 7 then '< 1 week'
    when days_in_status < 30 then '1-4 weeks'
    when days_in_status < 90 then '1-3 months'
    when days_in_status < 180 then '3-6 months'
    else '> 6 months'
  end as age_bucket,
  count(*) as ticket_count,
  round(avg(days_in_status), 1) as avg_days,
  round(100.0 * count(*) / sum(count(*)) over (), 1) as pct_of_total
from motherduck.ticket_aging
group by 1
order by min(days_in_status);
```

<BarChart
  data={age_buckets}
  x=age_bucket
  y=ticket_count
  title="Tickets by Age Category"
/>

<DataTable data={age_buckets}>
  <Column id=age_bucket/>
  <Column id=ticket_count fmt='#,##0'/>
  <Column id=avg_days fmt='0.0'/>
  <Column id=pct_of_total fmt='0.0"%"' contentType=colorscale scaleColor=orange/>
</DataTable>

---

## 🔴 Critical Tickets (>90 days)

```sql critical_tickets
select
  '[' || issue_key || '](https://validis.atlassian.net/browse/' || issue_key || ')' as issue_link,
  assignee,
  status,
  days_in_status
from motherduck.ticket_aging
where days_in_status > 90
order by days_in_status desc;
```

<DataTable data={critical_tickets} search=true>
  <Column id=issue_link title="Issue Key" contentType=link/>
  <Column id=assignee/>
  <Column id=status/>
  <Column id=days_in_status fmt='#,##0' contentType=colorscale scaleColor=red/>
</DataTable>

---

## 👤 Aging by Assignee

```sql aging_by_assignee
select
  assignee,
  count(*) as ticket_count,
  round(avg(days_in_status), 1) as avg_age,
  max(days_in_status) as max_age,
  count(case when days_in_status > 90 then 1 end) as critical_count
from motherduck.ticket_aging
where assignee is not null
group by assignee
order by avg_age desc;
```

<BarChart
  data={aging_by_assignee}
  x=assignee
  y=avg_age
  swapXY=true
  title="Average Ticket Age by Assignee"
/>

<DataTable data={aging_by_assignee} rows=10>
  <Column id=assignee/>
  <Column id=ticket_count fmt='#,##0'/>
  <Column id=avg_age fmt='0.0' contentType=colorscale scaleColor=orange/>
  <Column id=max_age fmt='#,##0' contentType=colorscale scaleColor=red/>
  <Column id=critical_count fmt='#,##0'/>
</DataTable>

---

## 📍 Status Breakdown

```sql status_breakdown
select
  status,
  count(*) as ticket_count,
  round(avg(days_in_status), 1) as avg_age,
  round(100.0 * count(*) / sum(count(*)) over (), 1) as pct_of_total
from motherduck.ticket_aging
group by status
order by ticket_count desc;
```

<BarChart
  data={status_breakdown}
  x=status
  y=ticket_count
  swapXY=true
  title="Tickets by Status"
/>

<DataTable data={status_breakdown}>
  <Column id=status/>
  <Column id=ticket_count fmt='#,##0'/>
  <Column id=avg_age fmt='0.0'/>
  <Column id=pct_of_total fmt='0.0"%"'/>
</DataTable>

---

## 🔍 Top 20 Oldest Tickets

```sql oldest_tickets
select
  '[' || issue_key || '](https://validis.atlassian.net/browse/' || issue_key || ')' as issue_link,
  assignee,
  status,
  days_in_status,
  case
    when days_in_status > 180 then 'Critical'
    when days_in_status > 90 then 'High'
    when days_in_status > 30 then 'Medium'
    else 'Low'
  end as priority
from motherduck.ticket_aging
order by days_in_status desc
limit 20;
```

<DataTable data={oldest_tickets}>
  <Column id=issue_link title="Issue Key" contentType=link/>
  <Column id=assignee/>
  <Column id=status/>
  <Column id=days_in_status fmt='#,##0' contentType=colorscale scaleColor=red/>
  <Column id=priority/>
</DataTable>
