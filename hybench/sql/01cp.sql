select sourceid,targetid,
case when sourceid= 177599 then 'outbound' when targetid= 177599 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 177599 or targetid = 177599
group by sourceid,targetid
order by total_amount desc;