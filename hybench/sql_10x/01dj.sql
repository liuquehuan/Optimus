select sourceid,targetid,
case when sourceid= 1121105 then 'outbound' when targetid= 1121105 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1121105 or targetid = 1121105
group by sourceid,targetid
order by total_amount desc;