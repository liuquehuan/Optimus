select sourceid,targetid,
case when sourceid= 200754 then 'outbound' when targetid= 200754 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 200754 or targetid = 200754
group by sourceid,targetid
order by total_amount desc;