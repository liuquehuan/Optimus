select sourceid,targetid,
case when sourceid= 1262765 then 'outbound' when targetid= 1262765 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1262765 or targetid = 1262765
group by sourceid,targetid
order by total_amount desc;