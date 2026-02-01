select sourceid,targetid,
case when sourceid= 269731 then 'outbound' when targetid= 269731 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 269731 or targetid = 269731
group by sourceid,targetid
order by total_amount desc;