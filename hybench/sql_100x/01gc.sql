select sourceid,targetid,
case when sourceid= 10999277 then 'outbound' when targetid= 10999277 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 10999277 or targetid = 10999277
group by sourceid,targetid
order by total_amount desc;