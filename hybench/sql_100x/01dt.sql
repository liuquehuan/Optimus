select sourceid,targetid,
case when sourceid= 21415113 then 'outbound' when targetid= 21415113 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21415113 or targetid = 21415113
group by sourceid,targetid
order by total_amount desc;