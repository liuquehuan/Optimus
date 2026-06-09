select sourceid,targetid,
case when sourceid= 374349 then 'outbound' when targetid= 374349 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 374349 or targetid = 374349
group by sourceid,targetid
order by total_amount desc;