select sourceid,targetid,
case when sourceid= 1529672 then 'outbound' when targetid= 1529672 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1529672 or targetid = 1529672
group by sourceid,targetid
order by total_amount desc;