select sourceid,targetid,
case when sourceid= 1481085 then 'outbound' when targetid= 1481085 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1481085 or targetid = 1481085
group by sourceid,targetid
order by total_amount desc;