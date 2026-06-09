select sourceid,targetid,
case when sourceid= 139087 then 'outbound' when targetid= 139087 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 139087 or targetid = 139087
group by sourceid,targetid
order by total_amount desc;