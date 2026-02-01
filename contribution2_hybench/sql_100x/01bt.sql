select sourceid,targetid,
case when sourceid= 10775383 then 'outbound' when targetid= 10775383 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 10775383 or targetid = 10775383
group by sourceid,targetid
order by total_amount desc;