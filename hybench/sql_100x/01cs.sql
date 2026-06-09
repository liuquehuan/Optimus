select sourceid,targetid,
case when sourceid= 18903340 then 'outbound' when targetid= 18903340 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 18903340 or targetid = 18903340
group by sourceid,targetid
order by total_amount desc;