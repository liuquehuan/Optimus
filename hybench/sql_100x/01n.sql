select sourceid,targetid,
case when sourceid= 16767371 then 'outbound' when targetid= 16767371 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16767371 or targetid = 16767371
group by sourceid,targetid
order by total_amount desc;