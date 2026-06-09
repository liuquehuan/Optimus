select sourceid,targetid,
case when sourceid= 329237 then 'outbound' when targetid= 329237 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 329237 or targetid = 329237
group by sourceid,targetid
order by total_amount desc;