select sourceid,targetid,
case when sourceid= 1609156 then 'outbound' when targetid= 1609156 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1609156 or targetid = 1609156
group by sourceid,targetid
order by total_amount desc;