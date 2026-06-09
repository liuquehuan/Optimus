select sourceid,targetid,
case when sourceid= 64525 then 'outbound' when targetid= 64525 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 64525 or targetid = 64525
group by sourceid,targetid
order by total_amount desc;