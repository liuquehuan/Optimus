select sourceid,targetid,
case when sourceid= 2386509 then 'outbound' when targetid= 2386509 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2386509 or targetid = 2386509
group by sourceid,targetid
order by total_amount desc;