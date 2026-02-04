select sourceid,targetid,
case when sourceid= 24915276 then 'outbound' when targetid= 24915276 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 24915276 or targetid = 24915276
group by sourceid,targetid
order by total_amount desc;