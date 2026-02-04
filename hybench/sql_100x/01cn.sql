select sourceid,targetid,
case when sourceid= 19654494 then 'outbound' when targetid= 19654494 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 19654494 or targetid = 19654494
group by sourceid,targetid
order by total_amount desc;