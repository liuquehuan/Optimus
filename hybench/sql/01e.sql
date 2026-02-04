select sourceid,targetid,
case when sourceid= 220388 then 'outbound' when targetid= 220388 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 220388 or targetid = 220388
group by sourceid,targetid
order by total_amount desc;