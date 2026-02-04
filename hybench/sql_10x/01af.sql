select sourceid,targetid,
case when sourceid= 2288895 then 'outbound' when targetid= 2288895 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2288895 or targetid = 2288895
group by sourceid,targetid
order by total_amount desc;