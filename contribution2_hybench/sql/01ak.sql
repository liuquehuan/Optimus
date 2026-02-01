select sourceid,targetid,
case when sourceid= 136352 then 'outbound' when targetid= 136352 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 136352 or targetid = 136352
group by sourceid,targetid
order by total_amount desc;