select sourceid,targetid,
case when sourceid= 15808438 then 'outbound' when targetid= 15808438 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 15808438 or targetid = 15808438
group by sourceid,targetid
order by total_amount desc;