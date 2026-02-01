select sourceid,targetid,
case when sourceid= 2328110 then 'outbound' when targetid= 2328110 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2328110 or targetid = 2328110
group by sourceid,targetid
order by total_amount desc;