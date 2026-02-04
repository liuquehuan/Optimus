select sourceid,targetid,
case when sourceid= 2588347 then 'outbound' when targetid= 2588347 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2588347 or targetid = 2588347
group by sourceid,targetid
order by total_amount desc;