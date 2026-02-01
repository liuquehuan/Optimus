select sourceid,targetid,
case when sourceid= 28643524 then 'outbound' when targetid= 28643524 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28643524 or targetid = 28643524
group by sourceid,targetid
order by total_amount desc;