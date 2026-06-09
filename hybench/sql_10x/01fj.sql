select sourceid,targetid,
case when sourceid= 119408 then 'outbound' when targetid= 119408 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 119408 or targetid = 119408
group by sourceid,targetid
order by total_amount desc;