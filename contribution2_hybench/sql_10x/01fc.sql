select sourceid,targetid,
case when sourceid= 106423 then 'outbound' when targetid= 106423 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 106423 or targetid = 106423
group by sourceid,targetid
order by total_amount desc;