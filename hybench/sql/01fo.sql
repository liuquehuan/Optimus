select sourceid,targetid,
case when sourceid= 248536 then 'outbound' when targetid= 248536 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 248536 or targetid = 248536
group by sourceid,targetid
order by total_amount desc;