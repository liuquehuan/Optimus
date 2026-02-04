select sourceid,targetid,
case when sourceid= 26617430 then 'outbound' when targetid= 26617430 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 26617430 or targetid = 26617430
group by sourceid,targetid
order by total_amount desc;