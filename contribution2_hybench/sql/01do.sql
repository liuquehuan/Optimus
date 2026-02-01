select sourceid,targetid,
case when sourceid= 248475 then 'outbound' when targetid= 248475 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 248475 or targetid = 248475
group by sourceid,targetid
order by total_amount desc;