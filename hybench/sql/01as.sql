select sourceid,targetid,
case when sourceid= 50017 then 'outbound' when targetid= 50017 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 50017 or targetid = 50017
group by sourceid,targetid
order by total_amount desc;