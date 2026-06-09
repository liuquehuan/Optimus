select sourceid,targetid,
case when sourceid= 17819213 then 'outbound' when targetid= 17819213 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17819213 or targetid = 17819213
group by sourceid,targetid
order by total_amount desc;