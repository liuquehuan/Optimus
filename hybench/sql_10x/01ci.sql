select sourceid,targetid,
case when sourceid= 165154 then 'outbound' when targetid= 165154 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 165154 or targetid = 165154
group by sourceid,targetid
order by total_amount desc;