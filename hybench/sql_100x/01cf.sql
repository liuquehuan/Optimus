select sourceid,targetid,
case when sourceid= 20307567 then 'outbound' when targetid= 20307567 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 20307567 or targetid = 20307567
group by sourceid,targetid
order by total_amount desc;