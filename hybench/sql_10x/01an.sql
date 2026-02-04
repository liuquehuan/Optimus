select sourceid,targetid,
case when sourceid= 1351599 then 'outbound' when targetid= 1351599 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1351599 or targetid = 1351599
group by sourceid,targetid
order by total_amount desc;