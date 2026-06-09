select sourceid,targetid,
case when sourceid= 1100137 then 'outbound' when targetid= 1100137 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1100137 or targetid = 1100137
group by sourceid,targetid
order by total_amount desc;