select sourceid,targetid,
case when sourceid= 16929290 then 'outbound' when targetid= 16929290 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16929290 or targetid = 16929290
group by sourceid,targetid
order by total_amount desc;