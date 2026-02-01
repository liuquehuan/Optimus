select sourceid,targetid,
case when sourceid= 3123368 then 'outbound' when targetid= 3123368 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 3123368 or targetid = 3123368
group by sourceid,targetid
order by total_amount desc;