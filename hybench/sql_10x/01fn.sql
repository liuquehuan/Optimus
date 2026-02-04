select sourceid,targetid,
case when sourceid= 2113056 then 'outbound' when targetid= 2113056 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2113056 or targetid = 2113056
group by sourceid,targetid
order by total_amount desc;