select sourceid,targetid,
case when sourceid= 6133327 then 'outbound' when targetid= 6133327 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6133327 or targetid = 6133327
group by sourceid,targetid
order by total_amount desc;