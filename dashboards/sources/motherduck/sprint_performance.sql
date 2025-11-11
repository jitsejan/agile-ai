-- Data is already filtered to board_id = 70 (Data Team Board) in the gold layer
select *
from gold.sprint_performance
order by start_date desc;
