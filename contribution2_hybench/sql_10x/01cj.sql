select sourceid,targetid,
case when sourceid= 728463 then 'outbound' when targetid= 728463 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 728463 or targetid = 728463
group by sourceid,targetid
order by total_amount desc;