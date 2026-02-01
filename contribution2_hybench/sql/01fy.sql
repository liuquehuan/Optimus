select sourceid,targetid,
case when sourceid= 172647 then 'outbound' when targetid= 172647 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 172647 or targetid = 172647
group by sourceid,targetid
order by total_amount desc;