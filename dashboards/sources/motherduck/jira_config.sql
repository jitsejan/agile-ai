select
    instance_name,
    project_key,
    jira_base_url
from gold.jira_config
order by project_key;
