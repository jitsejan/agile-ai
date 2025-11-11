-- Data is already filtered to board_id = 70 (Data Team Board) sprints in the gold layer
select *
from gold.sprint_carryover
order by changed_at desc;
