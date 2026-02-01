select sourceid,targetid,
case when sourceid= 2668457 then 'outbound' when targetid= 2668457 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2668457 or targetid = 2668457
group by sourceid,targetid
order by total_amount desc;