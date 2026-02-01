select sourceid,targetid,
case when sourceid= 275497 then 'outbound' when targetid= 275497 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 275497 or targetid = 275497
group by sourceid,targetid
order by total_amount desc;