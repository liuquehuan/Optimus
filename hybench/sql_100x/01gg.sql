select sourceid,targetid,
case when sourceid= 5413564 then 'outbound' when targetid= 5413564 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 5413564 or targetid = 5413564
group by sourceid,targetid
order by total_amount desc;