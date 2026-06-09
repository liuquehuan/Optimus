select sourceid,targetid,
case when sourceid= 1265362 then 'outbound' when targetid= 1265362 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1265362 or targetid = 1265362
group by sourceid,targetid
order by total_amount desc;