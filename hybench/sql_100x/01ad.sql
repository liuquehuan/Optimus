select sourceid,targetid,
case when sourceid= 11715555 then 'outbound' when targetid= 11715555 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 11715555 or targetid = 11715555
group by sourceid,targetid
order by total_amount desc;