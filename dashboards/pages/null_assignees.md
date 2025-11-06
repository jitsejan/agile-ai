
---
title: User Insights – Quality Checks
description: Identify unassigned issues and overall issue distribution from the gold schema in MotherDuck.
source: motherduck
---

## 🧩 Overview

This dashboard queries your **MotherDuck `gold` schema** (dbt Gold layer)  
and highlights where data quality needs attention — specifically rows where
`assignee` is missing.

---

### 🚨 Unassigned Issues

```sql null_assignees
SELECT
  assignee,
  issues_assigned,
  issues_completed
FROM motherduck.user_insights
WHERE assignee IS NULL
ORDER BY issues_assigned DESC
LIMIT 50;
```

<DataTable data={null_assignees} />
