select sourceid,targetid,
case when sourceid= 246070 then 'outbound' when targetid= 246070 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 246070 or targetid = 246070
group by sourceid,targetid
order by total_amount desc;