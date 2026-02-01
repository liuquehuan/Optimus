select sourceid,targetid,
case when sourceid= 13868150 then 'outbound' when targetid= 13868150 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 13868150 or targetid = 13868150
group by sourceid,targetid
order by total_amount desc;