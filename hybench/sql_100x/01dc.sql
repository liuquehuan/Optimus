select sourceid,targetid,
case when sourceid= 24374588 then 'outbound' when targetid= 24374588 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 24374588 or targetid = 24374588
group by sourceid,targetid
order by total_amount desc;