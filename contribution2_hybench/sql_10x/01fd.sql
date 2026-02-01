select sourceid,targetid,
case when sourceid= 2740429 then 'outbound' when targetid= 2740429 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2740429 or targetid = 2740429
group by sourceid,targetid
order by total_amount desc;