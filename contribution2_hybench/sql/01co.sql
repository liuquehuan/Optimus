select sourceid,targetid,
case when sourceid= 212062 then 'outbound' when targetid= 212062 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 212062 or targetid = 212062
group by sourceid,targetid
order by total_amount desc;