select sourceid,targetid,
case when sourceid= 2458293 then 'outbound' when targetid= 2458293 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2458293 or targetid = 2458293
group by sourceid,targetid
order by total_amount desc;