select sourceid,targetid,
case when sourceid= 80303 then 'outbound' when targetid= 80303 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 80303 or targetid = 80303
group by sourceid,targetid
order by total_amount desc;