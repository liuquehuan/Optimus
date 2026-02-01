select sourceid,targetid,
case when sourceid= 150421 then 'outbound' when targetid= 150421 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 150421 or targetid = 150421
group by sourceid,targetid
order by total_amount desc;