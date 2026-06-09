select sourceid,targetid,
case when sourceid= 214263 then 'outbound' when targetid= 214263 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 214263 or targetid = 214263
group by sourceid,targetid
order by total_amount desc;