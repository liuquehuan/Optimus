select sourceid,targetid,
case when sourceid= 40713 then 'outbound' when targetid= 40713 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 40713 or targetid = 40713
group by sourceid,targetid
order by total_amount desc;