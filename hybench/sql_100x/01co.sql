select sourceid,targetid,
case when sourceid= 373804 then 'outbound' when targetid= 373804 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 373804 or targetid = 373804
group by sourceid,targetid
order by total_amount desc;