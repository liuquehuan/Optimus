select sourceid,targetid,
case when sourceid= 133460 then 'outbound' when targetid= 133460 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 133460 or targetid = 133460
group by sourceid,targetid
order by total_amount desc;