select sourceid,targetid,
case when sourceid= 36611 then 'outbound' when targetid= 36611 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 36611 or targetid = 36611
group by sourceid,targetid
order by total_amount desc;