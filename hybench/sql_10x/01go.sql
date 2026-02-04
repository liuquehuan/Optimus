select sourceid,targetid,
case when sourceid= 1023450 then 'outbound' when targetid= 1023450 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1023450 or targetid = 1023450
group by sourceid,targetid
order by total_amount desc;