# 🚀 Agile AI - Jira Analytics

AI-powered Agile & DevOps insights with dbt + Evidence.dev + DuckDB.

## Quick Start

```bash
# Start the dashboard
make dashboard
```

Opens **http://localhost:3000** with your Jira analytics!

## Complete Workflow

```bash
# 1. Fetch data from Jira
uv run jira_pipeline

# 2. Transform with dbt  
make dbt-run

# 3. View dashboard
make dashboard

# Or refresh everything at once:
make refresh
```

## What You Get

Beautiful dashboards with:
- ✅ Sprint velocity trends
- ✅ Team member performance
- ✅ Personal activity tracking
- ✅ Ticket health alerts
- ✅ Carryover analysis

## Available Commands

```bash
make help              # Show all commands
make dashboard         # Start Evidence dev server
make dbt-run          # Run dbt transformations
make refresh          # Fetch Jira data + run dbt
```

See `make help` for full command list.

## Tech Stack

- **Storage**: DuckDB
- **Transformation**: dbt (10 gold models)
- **Visualization**: Evidence.dev (markdown + SQL)

## Documentation

- **FINAL_SOLUTION.md** - Complete architecture
- **EVIDENCE_SETUP.md** - Dashboard guide
- **Makefile** - All commands (run `make help`)

---

**Get started**: `make dashboard` → http://localhost:3000 🎉
