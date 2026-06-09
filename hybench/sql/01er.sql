select sourceid,targetid,
case when sourceid= 113465 then 'outbound' when targetid= 113465 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 113465 or targetid = 113465
group by sourceid,targetid
order by total_amount desc;