select sourceid,targetid,
case when sourceid= 21027901 then 'outbound' when targetid= 21027901 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21027901 or targetid = 21027901
group by sourceid,targetid
order by total_amount desc;