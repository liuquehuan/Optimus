select sourceid,targetid,
case when sourceid= 236547 then 'outbound' when targetid= 236547 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 236547 or targetid = 236547
group by sourceid,targetid
order by total_amount desc;