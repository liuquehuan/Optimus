select sourceid,targetid,
case when sourceid= 3230282 then 'outbound' when targetid= 3230282 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 3230282 or targetid = 3230282
group by sourceid,targetid
order by total_amount desc;