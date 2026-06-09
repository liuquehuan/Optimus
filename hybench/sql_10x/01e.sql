select sourceid,targetid,
case when sourceid= 764311 then 'outbound' when targetid= 764311 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 764311 or targetid = 764311
group by sourceid,targetid
order by total_amount desc;