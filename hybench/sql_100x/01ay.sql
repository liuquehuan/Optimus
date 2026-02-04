select sourceid,targetid,
case when sourceid= 25637717 then 'outbound' when targetid= 25637717 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 25637717 or targetid = 25637717
group by sourceid,targetid
order by total_amount desc;