select sourceid,targetid,
case when sourceid= 362301 then 'outbound' when targetid= 362301 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 362301 or targetid = 362301
group by sourceid,targetid
order by total_amount desc;