select sourceid,targetid,
case when sourceid= 2722349 then 'outbound' when targetid= 2722349 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2722349 or targetid = 2722349
group by sourceid,targetid
order by total_amount desc;