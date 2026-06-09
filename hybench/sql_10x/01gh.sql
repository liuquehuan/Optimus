select sourceid,targetid,
case when sourceid= 2451073 then 'outbound' when targetid= 2451073 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2451073 or targetid = 2451073
group by sourceid,targetid
order by total_amount desc;