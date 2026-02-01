select sourceid,targetid,
case when sourceid= 2644464 then 'outbound' when targetid= 2644464 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2644464 or targetid = 2644464
group by sourceid,targetid
order by total_amount desc;