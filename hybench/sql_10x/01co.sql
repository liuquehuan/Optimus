select sourceid,targetid,
case when sourceid= 2581040 then 'outbound' when targetid= 2581040 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2581040 or targetid = 2581040
group by sourceid,targetid
order by total_amount desc;