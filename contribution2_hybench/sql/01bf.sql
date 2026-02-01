select sourceid,targetid,
case when sourceid= 224910 then 'outbound' when targetid= 224910 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 224910 or targetid = 224910
group by sourceid,targetid
order by total_amount desc;