select sourceid,targetid,
case when sourceid= 7317457 then 'outbound' when targetid= 7317457 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 7317457 or targetid = 7317457
group by sourceid,targetid
order by total_amount desc;