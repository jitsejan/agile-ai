-- Silver model: Cleaned Jira sprints
select
    id as sprint_id,
    name as sprint_name,
    state as sprint_state,
    start_date,
    end_date,
    complete_date,
    board_id,
    goal
from {{ source('jira', 'all_sprints') }}
