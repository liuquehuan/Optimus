select sourceid,targetid,
case when sourceid= 41090 then 'outbound' when targetid= 41090 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 41090 or targetid = 41090
group by sourceid,targetid
order by total_amount desc;