select sourceid,targetid,
case when sourceid= 209429 then 'outbound' when targetid= 209429 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 209429 or targetid = 209429
group by sourceid,targetid
order by total_amount desc;