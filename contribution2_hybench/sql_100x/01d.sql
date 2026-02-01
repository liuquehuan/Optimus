select sourceid,targetid,
case when sourceid= 29311318 then 'outbound' when targetid= 29311318 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 29311318 or targetid = 29311318
group by sourceid,targetid
order by total_amount desc;