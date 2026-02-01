select sourceid,targetid,
case when sourceid= 28086019 then 'outbound' when targetid= 28086019 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28086019 or targetid = 28086019
group by sourceid,targetid
order by total_amount desc;