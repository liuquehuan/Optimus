select sourceid,targetid,
case when sourceid= 714284 then 'outbound' when targetid= 714284 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 714284 or targetid = 714284
group by sourceid,targetid
order by total_amount desc;