select sourceid,targetid,
case when sourceid= 892572 then 'outbound' when targetid= 892572 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 892572 or targetid = 892572
group by sourceid,targetid
order by total_amount desc;