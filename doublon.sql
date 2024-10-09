select count(global_event_id) as nb, global_event_id
from event
group by global_event_id
having count(global_event_id) > 1