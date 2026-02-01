select sourceid,targetid,
case when sourceid= 293643 then 'outbound' when targetid= 293643 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 293643 or targetid = 293643
group by sourceid,targetid
order by total_amount desc;