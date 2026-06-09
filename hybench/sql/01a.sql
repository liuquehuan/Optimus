select sourceid,targetid,
case when sourceid= 84997 then 'outbound' when targetid= 84997 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 84997 or targetid = 84997
group by sourceid,targetid
order by total_amount desc;