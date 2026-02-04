select sourceid,targetid,
case when sourceid= 2469296 then 'outbound' when targetid= 2469296 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2469296 or targetid = 2469296
group by sourceid,targetid
order by total_amount desc;