select sourceid,targetid,
case when sourceid= 1730802 then 'outbound' when targetid= 1730802 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1730802 or targetid = 1730802
group by sourceid,targetid
order by total_amount desc;