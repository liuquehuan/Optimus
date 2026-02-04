select sourceid,targetid,
case when sourceid= 1006424 then 'outbound' when targetid= 1006424 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1006424 or targetid = 1006424
group by sourceid,targetid
order by total_amount desc;