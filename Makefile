.PHONY: dbt-run dbt-test dbt-build dbt-debug dbt-clean evidence-dev evidence-build evidence-install all refresh

DBT_PROJECT_DIR := transform
DBT_PROFILES_DIR := transform
DBT_DUCKDB_PATH := $(CURDIR)/jira_pipeline.duckdb
EVIDENCE_DIR := dashboards

dbt-run:
	uv run dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-test:
	uv run dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-build:
	uv run dbt build --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-debug:
	uv run dbt debug --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-clean:
	uv run dbt clean --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-deps:
	uv run dbt deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-compile:
	uv run dbt compile --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-docs-generate:
	uv run dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-docs-serve:
	uv run dbt docs serve --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

# Evidence dashboard commands
evidence-install:
	cd $(EVIDENCE_DIR) && npm install

evidence-sources:
	cd $(EVIDENCE_DIR) && npm run sources

evidence-dev:
	cd $(EVIDENCE_DIR) && npm run sources && npm run dev

evidence-build:
	cd $(EVIDENCE_DIR) && npm run sources && npm run build

evidence-preview:
	cd $(EVIDENCE_DIR) && npm run preview

# Convenience commands
all: dbt-run evidence-dev

refresh:
	uv run jira_pipeline
	$(MAKE) dbt-run
	@echo "✅ Data refreshed! Reload your Evidence dashboard in the browser."

dashboard: evidence-dev

help:
	@echo "🚀 Agile AI - Available Commands"
	@echo ""
	@echo "📊 Evidence Dashboard:"
	@echo "  make evidence-install   - Install Evidence dependencies"
	@echo "  make evidence-sources   - Build Evidence data sources"
	@echo "  make evidence-dev       - Start Evidence dev server (http://localhost:3000)"
	@echo "  make evidence-build     - Build static Evidence site"
	@echo "  make evidence-preview   - Preview production build"
	@echo "  make dashboard          - Alias for evidence-dev"
	@echo ""
	@echo "🔄 dbt Commands:"
	@echo "  make dbt-run           - Run all dbt models"
	@echo "  make dbt-test          - Run dbt tests"
	@echo "  make dbt-build         - Build and test models"
	@echo "  make dbt-debug         - Debug dbt configuration"
	@echo "  make dbt-clean         - Clean dbt artifacts"
	@echo ""
	@echo "⚡ Convenience:"
	@echo "  make refresh           - Fetch Jira data + run dbt"
	@echo "  make all               - Run dbt + start Evidence"
	@echo "  make help              - Show this help message"
