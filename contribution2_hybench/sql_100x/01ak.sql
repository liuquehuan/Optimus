select sourceid,targetid,
case when sourceid= 27974492 then 'outbound' when targetid= 27974492 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 27974492 or targetid = 27974492
group by sourceid,targetid
order by total_amount desc;