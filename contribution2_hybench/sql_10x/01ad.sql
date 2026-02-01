select sourceid,targetid,
case when sourceid= 2206500 then 'outbound' when targetid= 2206500 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2206500 or targetid = 2206500
group by sourceid,targetid
order by total_amount desc;