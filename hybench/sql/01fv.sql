select sourceid,targetid,
case when sourceid= 30793 then 'outbound' when targetid= 30793 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 30793 or targetid = 30793
group by sourceid,targetid
order by total_amount desc;