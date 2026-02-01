select sourceid,targetid,
case when sourceid= 49513 then 'outbound' when targetid= 49513 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 49513 or targetid = 49513
group by sourceid,targetid
order by total_amount desc;