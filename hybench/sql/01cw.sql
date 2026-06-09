select sourceid,targetid,
case when sourceid= 251236 then 'outbound' when targetid= 251236 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 251236 or targetid = 251236
group by sourceid,targetid
order by total_amount desc;