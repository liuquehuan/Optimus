select sourceid,targetid,
case when sourceid= 6922773 then 'outbound' when targetid= 6922773 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6922773 or targetid = 6922773
group by sourceid,targetid
order by total_amount desc;