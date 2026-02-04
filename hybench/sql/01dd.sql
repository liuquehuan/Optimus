select sourceid,targetid,
case when sourceid= 94327 then 'outbound' when targetid= 94327 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 94327 or targetid = 94327
group by sourceid,targetid
order by total_amount desc;