import json
import os
import requests
import dlt
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# ✅ Load environment variables
load_dotenv()

# ✅ Jira configuration
JIRA_API_URL = os.getenv("JIRA_SERVER", "https://validis.atlassian.net")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")

# ✅ Initialize DLT pipeline (no explicit credentials, uses environment variables)
pipeline = dlt.pipeline(
    pipeline_name="jira_pipeline",
    destination="motherduck",
    dataset_name="jira_issues"
)

def get_jira_issues(project_key: str):
    """
    Retrieve all issues for a specified Jira project.
    
    Args:
        project_key: The key of the Jira project.
    
    Returns:
        A list of issues.
    """
    url = f"{JIRA_API_URL}/rest/api/3/search"
    all_issues = []
    start_at = 0
    max_results = 100  # Max limit per request

    headers = {
        "Accept": "application/json"
    }
    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)

    while True:
        query = {
            "jql": f'project = "{project_key}" ORDER BY created DESC',
            "fields": ["summary", "description", "created", "updated", "issuetype", "status", "assignee"],
            "startAt": start_at,
            "maxResults": max_results
        }
        
        response = requests.get(url, params=query, headers=headers, auth=auth)

        if response.status_code == 200:
            data = response.json()
            all_issues.extend(data.get('issues', []))
            start_at += max_results

            # Break the loop if there are no more issues to fetch
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
    """Run the DLT pipeline and store Jira issues in MotherDuck"""
    print("🔄 Fetching Jira Issues...")
    load_info = pipeline.run(load_issues(), table_name="jira_issues", write_disposition="merge")
    print(f"✅ Load Information: {load_info}")

if __name__ == "__main__":
    main()