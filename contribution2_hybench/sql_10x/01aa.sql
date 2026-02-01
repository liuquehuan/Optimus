select sourceid,targetid,
case when sourceid= 2341206 then 'outbound' when targetid= 2341206 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2341206 or targetid = 2341206
group by sourceid,targetid
order by total_amount desc;