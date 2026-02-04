select sourceid,targetid,
case when sourceid= 21425382 then 'outbound' when targetid= 21425382 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21425382 or targetid = 21425382
group by sourceid,targetid
order by total_amount desc;