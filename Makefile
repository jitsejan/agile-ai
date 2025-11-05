.PHONY: dbt-run dbt-test dbt-build dbt-debug dbt-clean

DBT_PROJECT_DIR := transform
DBT_PROFILES_DIR := transform
DBT_DUCKDB_PATH := $(CURDIR)/jira_pipeline.duckdb

dbt-run:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-test:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-build:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt build --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-debug:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt debug --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-clean:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt clean --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-deps:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-compile:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt compile --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-docs-generate:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-docs-serve:
	DBT_DUCKDB_PATH=$(DBT_DUCKDB_PATH) uv run dbt docs serve --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)
