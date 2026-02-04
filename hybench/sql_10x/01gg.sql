select sourceid,targetid,
case when sourceid= 2751363 then 'outbound' when targetid= 2751363 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2751363 or targetid = 2751363
group by sourceid,targetid
order by total_amount desc;