select sourceid,targetid,
case when sourceid= 195325 then 'outbound' when targetid= 195325 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 195325 or targetid = 195325
group by sourceid,targetid
order by total_amount desc;