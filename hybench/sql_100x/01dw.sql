select sourceid,targetid,
case when sourceid= 4800348 then 'outbound' when targetid= 4800348 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 4800348 or targetid = 4800348
group by sourceid,targetid
order by total_amount desc;