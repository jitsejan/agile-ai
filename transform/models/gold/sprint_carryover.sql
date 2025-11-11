-- Gold model: Track tickets that moved between sprints (carryover/spillover)
-- Helps identify estimation issues and scope creep
-- Filtered to board_id = 70 (Data Team Board) sprints
with board_70_sprints as (
    select
        sprint_name,
        sprint_number
    from {{ ref('sprints') }}
    where board_id = 70
),
issue_sprint_changes as (
    select
        h.issue_id,
        h.issue_key,
        h.history_created as changed_at,
        h.history_author__display_name as changed_by,
        h.from_string as from_sprint,
        h.to_string as to_sprint
    from {{ source('jira', 'issue_histories') }} h
    where h.field = 'Sprint'
        and h.from_string is not null
        and h.to_string is not null
        -- Filter to only include changes involving board 70 sprints
        and (h.to_string in (select sprint_name from board_70_sprints)
             or h.from_string in (select sprint_name from board_70_sprints))
),
issue_details as (
    select
        i.issue_id,
        i.issue_key,
        i.summary,
        i.status,
        i.assignee,
        try_cast(i.fields__customfield_10014 as double) as story_points
    from {{ ref('issues') }} i
)
select
    sc.issue_id,
    sc.issue_key,
    id.summary,
    id.status,
    id.assignee,
    id.story_points,
    sc.changed_at,
    sc.changed_by,
    sc.from_sprint,
    sc.to_sprint,
    from_s.sprint_number as from_sprint_number,
    to_s.sprint_number as to_sprint_number,
    count(*) over (partition by sc.issue_id) as times_moved
from issue_sprint_changes sc
left join issue_details id on sc.issue_id = id.issue_id
left join board_70_sprints from_s on sc.from_sprint = from_s.sprint_name
left join board_70_sprints to_s on sc.to_sprint = to_s.sprint_name
order by from_s.sprint_number desc, sc.changed_at desc
