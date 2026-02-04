select sourceid,targetid,
case when sourceid= 4382 then 'outbound' when targetid= 4382 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 4382 or targetid = 4382
group by sourceid,targetid
order by total_amount desc;