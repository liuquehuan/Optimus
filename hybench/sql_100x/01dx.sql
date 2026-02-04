select sourceid,targetid,
case when sourceid= 13885599 then 'outbound' when targetid= 13885599 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 13885599 or targetid = 13885599
group by sourceid,targetid
order by total_amount desc;