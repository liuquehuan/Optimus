select sourceid,targetid,
case when sourceid= 289220 then 'outbound' when targetid= 289220 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 289220 or targetid = 289220
group by sourceid,targetid
order by total_amount desc;