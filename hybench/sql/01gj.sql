select sourceid,targetid,
case when sourceid= 75514 then 'outbound' when targetid= 75514 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 75514 or targetid = 75514
group by sourceid,targetid
order by total_amount desc;