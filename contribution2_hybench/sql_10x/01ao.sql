select sourceid,targetid,
case when sourceid= 1773320 then 'outbound' when targetid= 1773320 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1773320 or targetid = 1773320
group by sourceid,targetid
order by total_amount desc;