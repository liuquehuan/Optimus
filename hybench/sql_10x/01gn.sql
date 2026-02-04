select sourceid,targetid,
case when sourceid= 2484458 then 'outbound' when targetid= 2484458 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2484458 or targetid = 2484458
group by sourceid,targetid
order by total_amount desc;