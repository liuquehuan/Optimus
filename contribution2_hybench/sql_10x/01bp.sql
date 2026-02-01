select sourceid,targetid,
case when sourceid= 435319 then 'outbound' when targetid= 435319 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 435319 or targetid = 435319
group by sourceid,targetid
order by total_amount desc;