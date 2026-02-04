select sourceid,targetid,
case when sourceid= 7421261 then 'outbound' when targetid= 7421261 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 7421261 or targetid = 7421261
group by sourceid,targetid
order by total_amount desc;