select sourceid,targetid,
case when sourceid= 240283 then 'outbound' when targetid= 240283 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 240283 or targetid = 240283
group by sourceid,targetid
order by total_amount desc;