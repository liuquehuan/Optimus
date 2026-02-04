select sourceid,targetid,
case when sourceid= 26338503 then 'outbound' when targetid= 26338503 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 26338503 or targetid = 26338503
group by sourceid,targetid
order by total_amount desc;