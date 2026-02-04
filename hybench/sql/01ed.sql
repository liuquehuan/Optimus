select sourceid,targetid,
case when sourceid= 272436 then 'outbound' when targetid= 272436 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 272436 or targetid = 272436
group by sourceid,targetid
order by total_amount desc;