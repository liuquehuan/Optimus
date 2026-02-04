select sourceid,targetid,
case when sourceid= 2339024 then 'outbound' when targetid= 2339024 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2339024 or targetid = 2339024
group by sourceid,targetid
order by total_amount desc;