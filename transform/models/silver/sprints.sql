-- Silver model: Cleaned Jira sprints
select
    id as sprint_id,
    name as sprint_name,
    state as sprint_state,
    start_date,
    end_date,
    complete_date,
    board_id,
    goal,
    -- Extract sprint number for proper ordering (e.g., "Story Sprint 14" -> 14)
    try_cast(regexp_extract(name, '(\d+)$', 1) as integer) as sprint_number
from {{ source('jira', 'all_sprints') }}
