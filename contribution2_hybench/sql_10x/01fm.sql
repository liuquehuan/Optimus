select sourceid,targetid,
case when sourceid= 561962 then 'outbound' when targetid= 561962 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 561962 or targetid = 561962
group by sourceid,targetid
order by total_amount desc;