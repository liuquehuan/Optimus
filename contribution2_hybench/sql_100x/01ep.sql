select sourceid,targetid,
case when sourceid= 8824298 then 'outbound' when targetid= 8824298 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8824298 or targetid = 8824298
group by sourceid,targetid
order by total_amount desc;