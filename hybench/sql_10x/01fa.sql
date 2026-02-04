select sourceid,targetid,
case when sourceid= 2982789 then 'outbound' when targetid= 2982789 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2982789 or targetid = 2982789
group by sourceid,targetid
order by total_amount desc;