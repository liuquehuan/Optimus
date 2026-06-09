select sourceid,targetid,
case when sourceid= 1384165 then 'outbound' when targetid= 1384165 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1384165 or targetid = 1384165
group by sourceid,targetid
order by total_amount desc;