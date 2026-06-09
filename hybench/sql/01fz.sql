select sourceid,targetid,
case when sourceid= 127312 then 'outbound' when targetid= 127312 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 127312 or targetid = 127312
group by sourceid,targetid
order by total_amount desc;