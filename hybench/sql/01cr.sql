select sourceid,targetid,
case when sourceid= 2263 then 'outbound' when targetid= 2263 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2263 or targetid = 2263
group by sourceid,targetid
order by total_amount desc;