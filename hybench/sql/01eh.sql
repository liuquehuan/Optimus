select sourceid,targetid,
case when sourceid= 139637 then 'outbound' when targetid= 139637 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 139637 or targetid = 139637
group by sourceid,targetid
order by total_amount desc;