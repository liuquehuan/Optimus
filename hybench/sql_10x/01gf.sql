select sourceid,targetid,
case when sourceid= 920286 then 'outbound' when targetid= 920286 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 920286 or targetid = 920286
group by sourceid,targetid
order by total_amount desc;