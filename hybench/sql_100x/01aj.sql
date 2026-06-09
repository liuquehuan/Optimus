select sourceid,targetid,
case when sourceid= 16223652 then 'outbound' when targetid= 16223652 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16223652 or targetid = 16223652
group by sourceid,targetid
order by total_amount desc;