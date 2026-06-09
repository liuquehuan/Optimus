select sourceid,targetid,
case when sourceid= 158366 then 'outbound' when targetid= 158366 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 158366 or targetid = 158366
group by sourceid,targetid
order by total_amount desc;