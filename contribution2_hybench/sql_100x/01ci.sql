select sourceid,targetid,
case when sourceid= 4234966 then 'outbound' when targetid= 4234966 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 4234966 or targetid = 4234966
group by sourceid,targetid
order by total_amount desc;