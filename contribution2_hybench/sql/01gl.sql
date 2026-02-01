select sourceid,targetid,
case when sourceid= 288260 then 'outbound' when targetid= 288260 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 288260 or targetid = 288260
group by sourceid,targetid
order by total_amount desc;