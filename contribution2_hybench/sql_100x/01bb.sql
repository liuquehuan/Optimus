select sourceid,targetid,
case when sourceid= 11793032 then 'outbound' when targetid= 11793032 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 11793032 or targetid = 11793032
group by sourceid,targetid
order by total_amount desc;