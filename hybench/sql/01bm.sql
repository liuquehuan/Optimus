select sourceid,targetid,
case when sourceid= 179789 then 'outbound' when targetid= 179789 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 179789 or targetid = 179789
group by sourceid,targetid
order by total_amount desc;