select sourceid,targetid,
case when sourceid= 139099 then 'outbound' when targetid= 139099 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 139099 or targetid = 139099
group by sourceid,targetid
order by total_amount desc;