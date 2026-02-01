select sourceid,targetid,
case when sourceid= 2935569 then 'outbound' when targetid= 2935569 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2935569 or targetid = 2935569
group by sourceid,targetid
order by total_amount desc;