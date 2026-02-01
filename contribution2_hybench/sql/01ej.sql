select sourceid,targetid,
case when sourceid= 27301 then 'outbound' when targetid= 27301 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 27301 or targetid = 27301
group by sourceid,targetid
order by total_amount desc;