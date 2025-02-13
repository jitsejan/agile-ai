import json
import os
import requests
import dlt
import duckdb
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from datetime import datetime

# ✅ Load environment variables
load_dotenv()

# ✅ Jira configuration
JIRA_API_URL = os.getenv("JIRA_SERVER", "https://validis.atlassian.net")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")

# ✅ MotherDuck configuration
MOTHERDUCK_DB = os.getenv("DESTINATION__MOTHERDUCK__CREDENTIALS__DATABASE", "md:agileai_db")
MOTHERDUCK_TOKEN = os.getenv("DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD")

# ✅ Ensure MotherDuck token is set
if not MOTHERDUCK_TOKEN:
    raise ValueError("❌ ERROR: `MOTHERDUCK_TOKEN` is missing. Please set it in your `.env` file.")

# ✅ Initialize DLT pipeline (no explicit credentials, uses environment variables)
pipeline = dlt.pipeline(
    pipeline_name="jira_pipeline",
    destination="motherduck",
    dataset_name="jira_issues"
)

def format_timestamp(timestamp):
    """Format the timestamp to the required JQL format."""
    if timestamp:
        try:
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
            timestamp = timestamp.replace("Z", "+00:00")  

            dt = datetime.fromisoformat(timestamp)

            return dt.strftime('%Y-%m-%d %H:%M')
        except ValueError as e:
            print(f"❌ ERROR: Invalid timestamp format: {timestamp} - {e}")
            return None
    return None

def get_last_updated_timestamp():
    """Retrieve the last updated timestamp from MotherDuck, handling missing tables correctly."""
    conn = duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}")

    # ✅ Check if the table exists in the correct schema
    table_check_query = """
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'jira_issues' 
        AND table_name = 'jira_issues'
    """
    table_exists = conn.execute(table_check_query).fetchone()[0]
    print(table_exists)
    if not table_exists:
        print("⚠️ WARNING: Table `jira_issues.jira_issues` does not exist. Running full load.")
        return None  # ✅ Run full load when table is missing

    # ✅ Fetch last updated timestamp from the schema-prefixed table
    result = conn.execute("SELECT MAX(updated_date) FROM jira_issues.jira_issues").fetchone()
    
    return result[0] if result[0] else None

def get_jira_issues(project_key: str):
    """Retrieve all issues for a specified Jira project."""
    last_updated = get_last_updated_timestamp()
    formatted_last_updated = format_timestamp(last_updated)

    url = f"{JIRA_API_URL}/rest/api/3/search"
    all_issues = []
    start_at = 0
    max_results = 100  # Max limit per request

    headers = {"Accept": "application/json"}
    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)

    jql_filter = f'project = "{project_key}"'
    if formatted_last_updated:
        jql_filter += f' AND updated >= "{formatted_last_updated}" ORDER BY updated DESC'

    while True:
        query = {
            "jql": jql_filter,
            "fields": ["summary", "description", "created", "updated", "issuetype", "status", "assignee"],
            "startAt": start_at,
            "maxResults": max_results
        }

        response = requests.get(url, params=query, headers=headers, auth=auth)

        if response.status_code == 200:
            data = response.json()
            all_issues.extend(data.get('issues', []))
            start_at += max_results

            if start_at >= data.get('total', 0):
                break
        else:
            print(f"❌ ERROR: Failed to fetch issues: {response.status_code} - {response.text}")
            break

    return all_issues

def load_issues():
    """
    Load issues into the DLT pipeline for MotherDuck.
    """
    project_key = "DT"  # ✅ Ensure DT exists before running

    issues = get_jira_issues(project_key)

    for issue in issues:
        assignee = issue["fields"].get("assignee")
        assignee_name = assignee["displayName"] if assignee else None  # ✅ Handle None gracefully

        yield {
            "id": issue["id"],
            "key": issue["key"],
            "summary": issue["fields"].get("summary", ""),
            "status": issue["fields"].get("status", {}).get("name", ""),
            "assignee": assignee_name,
            "created_date": issue["fields"].get("created", ""),
            "updated_date": issue["fields"].get("updated", ""),
            "issue_type": issue["fields"].get("issuetype", {}).get("name", ""),
            "fields_json": json.dumps(issue["fields"])  # ✅ Store fields as JSON
        }

def main():
    """Run the DLT pipeline with incremental loading."""
    print("🔄 Fetching Jira Issues...")

    load_info = pipeline.run(
        load_issues(),
        table_name="jira_issues",
        write_disposition="merge",
        primary_key="id"
    )

    print(f"✅ Load Information: {load_info}")

if __name__ == "__main__":
    main()