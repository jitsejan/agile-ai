-- Gold model: Jira configuration
-- Maps project keys to their Jira instance base URLs
select
    instance_name,
    project_key,
    jira_base_url
from {{ ref('jira_config') }}
