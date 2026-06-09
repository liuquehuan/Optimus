select sourceid,targetid,
case when sourceid= 498558 then 'outbound' when targetid= 498558 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 498558 or targetid = 498558
group by sourceid,targetid
order by total_amount desc;