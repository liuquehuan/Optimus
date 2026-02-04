select sourceid,targetid,
case when sourceid= 169205 then 'outbound' when targetid= 169205 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 169205 or targetid = 169205
group by sourceid,targetid
order by total_amount desc;