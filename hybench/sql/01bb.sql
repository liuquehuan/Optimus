select sourceid,targetid,
case when sourceid= 211717 then 'outbound' when targetid= 211717 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 211717 or targetid = 211717
group by sourceid,targetid
order by total_amount desc;