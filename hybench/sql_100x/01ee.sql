select sourceid,targetid,
case when sourceid= 24150219 then 'outbound' when targetid= 24150219 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 24150219 or targetid = 24150219
group by sourceid,targetid
order by total_amount desc;