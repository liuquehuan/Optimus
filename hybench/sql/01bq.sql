select sourceid,targetid,
case when sourceid= 5671 then 'outbound' when targetid= 5671 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 5671 or targetid = 5671
group by sourceid,targetid
order by total_amount desc;