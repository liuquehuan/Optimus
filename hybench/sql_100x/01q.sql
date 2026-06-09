select sourceid,targetid,
case when sourceid= 24042296 then 'outbound' when targetid= 24042296 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 24042296 or targetid = 24042296
group by sourceid,targetid
order by total_amount desc;