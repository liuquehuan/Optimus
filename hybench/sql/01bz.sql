select sourceid,targetid,
case when sourceid= 54352 then 'outbound' when targetid= 54352 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 54352 or targetid = 54352
group by sourceid,targetid
order by total_amount desc;