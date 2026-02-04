select sourceid,targetid,
case when sourceid= 2583388 then 'outbound' when targetid= 2583388 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2583388 or targetid = 2583388
group by sourceid,targetid
order by total_amount desc;