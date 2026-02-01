select sourceid,targetid,
case when sourceid= 2808756 then 'outbound' when targetid= 2808756 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2808756 or targetid = 2808756
group by sourceid,targetid
order by total_amount desc;