select sourceid,targetid,
case when sourceid= 758530 then 'outbound' when targetid= 758530 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 758530 or targetid = 758530
group by sourceid,targetid
order by total_amount desc;