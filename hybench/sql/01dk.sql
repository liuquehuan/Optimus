select sourceid,targetid,
case when sourceid= 110359 then 'outbound' when targetid= 110359 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 110359 or targetid = 110359
group by sourceid,targetid
order by total_amount desc;