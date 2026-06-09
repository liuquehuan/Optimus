select sourceid,targetid,
case when sourceid= 214568 then 'outbound' when targetid= 214568 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 214568 or targetid = 214568
group by sourceid,targetid
order by total_amount desc;