select sourceid,targetid,
case when sourceid= 2551777 then 'outbound' when targetid= 2551777 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2551777 or targetid = 2551777
group by sourceid,targetid
order by total_amount desc;