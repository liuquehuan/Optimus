select sourceid,targetid,
case when sourceid= 2639152 then 'outbound' when targetid= 2639152 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2639152 or targetid = 2639152
group by sourceid,targetid
order by total_amount desc;