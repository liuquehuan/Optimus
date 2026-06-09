select sourceid,targetid,
case when sourceid= 21953566 then 'outbound' when targetid= 21953566 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21953566 or targetid = 21953566
group by sourceid,targetid
order by total_amount desc;