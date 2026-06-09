select sourceid,targetid,
case when sourceid= 44504 then 'outbound' when targetid= 44504 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 44504 or targetid = 44504
group by sourceid,targetid
order by total_amount desc;