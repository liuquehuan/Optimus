select sourceid,targetid,
case when sourceid= 180029 then 'outbound' when targetid= 180029 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 180029 or targetid = 180029
group by sourceid,targetid
order by total_amount desc;