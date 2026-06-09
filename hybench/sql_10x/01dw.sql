select sourceid,targetid,
case when sourceid= 2822354 then 'outbound' when targetid= 2822354 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2822354 or targetid = 2822354
group by sourceid,targetid
order by total_amount desc;