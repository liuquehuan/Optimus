select sourceid,targetid,
case when sourceid= 472769 then 'outbound' when targetid= 472769 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 472769 or targetid = 472769
group by sourceid,targetid
order by total_amount desc;