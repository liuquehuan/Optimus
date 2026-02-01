select sourceid,targetid,
case when sourceid= 139449 then 'outbound' when targetid= 139449 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 139449 or targetid = 139449
group by sourceid,targetid
order by total_amount desc;