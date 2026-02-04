select sourceid,targetid,
case when sourceid= 1129334 then 'outbound' when targetid= 1129334 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1129334 or targetid = 1129334
group by sourceid,targetid
order by total_amount desc;