select sourceid,targetid,
case when sourceid= 2954083 then 'outbound' when targetid= 2954083 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2954083 or targetid = 2954083
group by sourceid,targetid
order by total_amount desc;