import json
import os
import requests
import duckdb
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Jira configuration
jira_server = os.getenv('JIRA_SERVER')
jira_email = os.getenv('JIRA_EMAIL')
jira_api_token = os.getenv('JIRA_API_TOKEN')

# MotherDuck configuration
motherduck_db_name = os.getenv('MOTHERDUCK_DATABASE')
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
motherduck_connection_string = f'md:{motherduck_db_name}?motherduck_token={motherduck_token}'

def get_jira_issues(project_key: str):
    """
    Retrieve all issues for a specified Jira project.

    Args:
        project_key: The key of the Jira project.

    Returns:
        A list of issues.
    """
    url = f"{jira_server}/rest/api/2/search"
    all_issues = []
    start_at = 0
    max_results = 100  # You can set this to a maximum of 100

    while True:
        query = {
            'jql': f'project = "{project_key}" ORDER BY created DESC',
            'fields': '*all',  # Retrieve all fields
            'startAt': start_at,
            'maxResults': max_results
        }
        
        response = requests.get(url, params=query, auth=(jira_email, jira_api_token))
        
        if response.status_code == 200:
            data = response.json()
            all_issues.extend(data.get('issues', []))
            start_at += max_results
            
            # Break the loop if there are no more issues to fetch
            if start_at >= data.get('total', 0):
                break
        else:
            print(f"Failed to fetch issues: {response.status_code} - {response.text}")
            break

    return all_issues

def store_issues_in_motherduck(issues):
    """
    Store the retrieved issues into the MotherDuck database.

    Args:
        issues: A list of issues to store.
    """
    # Connect to MotherDuck
    conn = duckdb.connect(motherduck_connection_string)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS jira_issues (
        id INTEGER,
        key VARCHAR,
        summary VARCHAR,
        status VARCHAR,
        assignee VARCHAR,
        fields JSON  -- Add a column for the full fields JSON if needed
    )
    """)

    for issue in issues:
        assignee = issue['fields'].get('assignee')
        assignee_name = assignee['displayName'] if assignee else ''  # Check if assignee is None

        data = (
            issue['id'],
            issue['key'],
            issue['fields'].get('summary', ''),
            issue['fields'].get('status', {}).get('name', ''),
            assignee_name,
            json.dumps(issue['fields'])
        )
        conn.execute("INSERT INTO jira_issues (id, key, summary, status, assignee, fields) VALUES (?, ?, ?, ?, ?, ?)", data)

    conn.commit()
    conn.close()

def main():
    project_key = 'DT'
    issues = get_jira_issues(project_key)
    store_issues_in_motherduck(issues)

if __name__ == "__main__":
    main()