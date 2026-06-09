select sourceid,targetid,
case when sourceid= 299731 then 'outbound' when targetid= 299731 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 299731 or targetid = 299731
group by sourceid,targetid
order by total_amount desc;