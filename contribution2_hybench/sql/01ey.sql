select sourceid,targetid,
case when sourceid= 295800 then 'outbound' when targetid= 295800 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 295800 or targetid = 295800
group by sourceid,targetid
order by total_amount desc;